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

const driverAndDispatcher = {};

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

  for (const individualDispatcher of dispatcherIDs) {
    console.log(`Retrieving ${individualDispatcher}'s drivers`);

    const each_dispatcher_url = `${GETTING_DISPATCHERS_URL}/${individualDispatcher}/item`;

    console.log(`${individualDispatcher}'s API calling url`);

    const headers = {
      Authorization: `Ditat-Token ${apiToken}`
    };

    try{
    const { data } = await axios.get(each_dispatcher_url, { headers });
    console.log(`data=${data.data[0].recordId}`);

    const oneDispatcherDriversInformation = data.data
    oneDispatcherDriversInformation.forEach(element => {
      let driver = element.recordId;
      driverAndDispatcher[driver] = dispatcherIdAndName[individualDispatcher];
    });
    }catch(error){
      console.log("Error getting Dispatchers data:", error.message);
      throw error;
    }
  }
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
      await getAllDispatchersData(apiToken);

      const drivers = rawDrivers.map((d) => ([
        d.driverId,
        d.status,
        d.firstName,
        d.lastName,
        d.truckId,
        d.phoneNumber,
        d.emailAddress,
        d.hiredOn,
        d.updatedOn,
        d.companyId,
        driverAndDispatcher[d.driverId],
      ]));

      await upsertDrivers(drivers);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error:`, error.message);
    }
  }

  // Run once at startup, then every 6 hours
  fetchAndUpsertDrivers();
  setInterval(fetchAndUpsertDrivers, 6 * 60 * 60 * 1000); // 6 hours

  // (async () => {
  //   const apiToken = await getApiToken();
  //   await getAllDispatchersData(apiToken);
  //   console.log(driverAndDispatcher["523REGINALD"])
  // })();
