const axios = require("axios");
const mysql = require("mysql2/promise");
require("dotenv").config();

const {
  BASE_URL: BASE_URL,
  GETTING_TOKEN_URL: GETTING_TOKEN_URL,
  DB_HOST: DB_HOST,
  DB_USER: DB_USER,
  DB_PASSWORD: DB_PASSWORD,
  DB_NAME: DB_NAME,
} = process.env;

// Helper: Get API token
async function getApiToken() {
  console.log("Start for getting API token")
  const headers = {
    "Ditat-Application-Role": "Login to TMS",
    "ditat-account-id": "agylogistics",
    "Authorization": "Basic aG9zaGVscDp3RkxIbTYub2th",
  };
  const { data } = await axios.post(GETTING_TOKEN_URL, {}, { headers });
  if (data) {
    console.log(`API token= ${data}`)
  }
  return data;
}

// Helper: Fetch driver data
async function fetchDrivers(apiToken) {
  console.log("Start for retrieving drivers data from system")
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
  const { data } = await axios.post(BASE_URL, body, { headers });
  if (data) {
    console.log("Fetching drivers' data Successfully")
  }
  return data?.data?.data || [];
}

// Helper: Upsert drivers into DB
async function upsertDrivers(drivers) {
  const connection = await mysql.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
  });

  console.log("Database connection success")

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
      console.log(`Driver ${driverId} data processing success`)
    }
    console.log(`[${new Date().toISOString()}] Upsert complete!`);
    
  } catch (error) {
    console.error("Error during upsert:", error);
  } finally {
    await connection.end();
  }
}

// Main: Fetch and upsert process
async function fetchAndUpsertDrivers() {
  try {
    console.log(`[${new Date().toISOString()}] Starting fetch and upsert...`);
    const apiToken = await getApiToken();
    const rawDrivers = await fetchDrivers(apiToken);

    const drivers = rawDrivers.map((d) => ([
      d.driverId,
      d.Status,
      d.firstName,
      d.lastName,
      d.truckId,
      d.phoneNumber,
      d.emailAddress,
      d.hiredOn,
      d.updatedOn,
      d.companyId,
    ]));

    await upsertDrivers(drivers);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error:`, error.message);
  }
}

// Run once at startup, then every 6 hours
fetchAndUpsertDrivers();
setInterval(fetchAndUpsertDrivers, 6 * 60 * 60 * 1000); // 6 hours

