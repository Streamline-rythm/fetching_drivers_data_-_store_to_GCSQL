import axios from 'axios';
import mysql from "mysql2/promise";
import dotenv from 'dotenv';

dotenv.config();

const {
  GETTING_ALL_DRIVERS_URL,
  GETTING_DISPATCHERS_URL,
  GETTING_TOKEN_URL,
  DB_HOST,
  DB_USER,
  DB_NAME,
  DB_PASSWORD,
} = process.env;

const dispatcherIdAndName = {
  12: "Marko",
  28: "Mario",
  53: "Paul",
  57: "Milos",
  65: "Aleks",
  70: "Luka",
  72: "Adrian",
  78: "David",
  79: "Kevin",
  80: "Monte",
  81: "Austin",
};

const dispatcherIDs = [
  12, 28, 53, 57, 65, 70, 72, 78, 79, 80, 81
];

// Get API token
async function getApiToken() {
  console.log("Start for getting API token");
  const headers = {
    "Ditat-Application-Role": "Login to TMS",
    "ditat-account-id": "agylogistics",
    "Authorization": "Basic aG9zaGVscDp3RkxIbTYub2th",
  };
  try {
    const { data } = await axios.post(GETTING_TOKEN_URL, {}, { headers });
    if (data) {
      console.log(`API token= ${JSON.stringify(data)}`);
      return data;
    }
  } catch (error) {
    console.error("Error getting API token:", error.message);
    throw error;
  }
  return null;
}

// Fetch All drivers data
async function fetchDrivers(apiToken) {
  console.log("Start for retrieving drivers data from system");
  const headers = {
    Authorization: `Ditat-Token ${apiToken}`,
  };
  const body = {
    filterItems: [
      {
        columnName: "driverId",
        filterType: 5,
        filterFromValue: "",
      },
    ],
  };
  try {
    const { data } = await axios.post(GETTING_ALL_DRIVERS_URL, body, { headers });
    if (data) {
      console.log("Fetching drivers' data Successfully");
      return data?.data?.data || [];
    }
  } catch (error) {
    console.error("Error fetching drivers:", error.message);
    throw error;
  }
  return [];
}

// Fetch All Dispatchers data
async function getAllDispatchersData(apiToken) {
  console.log("Start retrieving Dispatchers data from system");
  const driverAndDispatcher = {};

  for (const individualDispatcher of dispatcherIDs) {
    console.log(`Retrieving ${individualDispatcher}'s drivers`);

    const each_dispatcher_url = `${GETTING_DISPATCHERS_URL}/${individualDispatcher}/item`;

    console.log(`${individualDispatcher}'s API calling url`);

    const headers = {
      Authorization: `Ditat-Token ${apiToken}`
    };

    try {
      const { data } = await axios.get(each_dispatcher_url, { headers });
      console.log(`data=${data.data[0].recordId}`);

      const oneDispatcherDriversInformation = data.data
      oneDispatcherDriversInformation.forEach(element => {
        let driver = element.recordId.trim();
        driverAndDispatcher[driver] = dispatcherIdAndName[individualDispatcher];
      });
    } catch (error) {
      console.log("Error getting Dispatchers data:", error.message);
      throw error;
    }
  }
  return driverAndDispatcher;
}
//  Upsert drivers into DB
async function upsertDrivers(drivers) {
  const connection = await mysql.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
  });

  console.log("Database connection success");

  const sql = `
    INSERT INTO drivers (driverId, driver_data)
    VALUES (?, ?)
    ON DUPLICATE KEY UPDATE driver_data = VALUES(driver_data)
  `;

  try {
    for (const driver of drivers) {
      const driverId = driver[0];
      const driverJson = JSON.stringify(driver);
      console.log(`Upserting driverId: ${driverId}`);
      await connection.execute(sql, [driverId, driverJson]);
      console.log(`Driver ${driverId} data processing success`);
    }
    console.log(`[${new Date().toISOString()}] Upsert complete!`);

  } catch (error) {
    console.error("Error during upsert:", error.message);
  } finally {
    await connection.end();
  }
}

// Fetch and upsert process
async function fetchAndUpsertDrivers() {
  try {
    console.log(`[${new Date().toISOString()}] Starting fetch and upsert...`);
    const apiToken = await getApiToken();
    const rawDrivers = await fetchDrivers(apiToken);
    const driverAndDispatcher = await getAllDispatchersData(apiToken);

    console.log(driverAndDispatcher);

    const drivers = rawDrivers.map((d) => {
      let convertedDriverId = d.driverId;
      let dispatcher = driverAndDispatcher[convertedDriverId];
      console.log(convertedDriverId);
      console.log(dispatcher);
      return (
        [convertedDriverId,
          d.status,
          d.firstName,
          d.lastName,
          d.truckId,
          d.phoneNumber,
          d.emailAddress,
          d.hiredOn,
          d.updatedOn,
          d.companyId,
          dispatcher,
        ]
      )
    });

    await upsertDrivers(drivers);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error:`, error.message);
  }
}

// --- Improved Scheduling Logic ---

// Function to schedule periodic updates
function scheduleDriverUpdates(intervalMs) {
  console.log(`Scheduling driver data update every ${intervalMs / (60 * 60 * 1000)} hours.`);
  let intervalId = setInterval(async () => {
    console.log('Scheduled update triggered.');
    await fetchAndUpsertDrivers();
  }, intervalMs);

  // Graceful shutdown
  const shutdown = () => {
    console.log('Shutting down, clearing scheduled updates.');
    clearInterval(intervalId);
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Run once at startup, then every 6 hours
(async () => {
  await fetchAndUpsertDrivers();
  scheduleDriverUpdates(6 * 60 * 60 * 1000); // 6 hours
})();
