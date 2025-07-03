const axios = require("axios");
const mysql = require("mysql2/promise");
require("dotenv").config();

const {
  BASE_URL: baseUrl,
  GETTING_TOEN_URL: gettingTokenUrl,
  DB_HOST: db_host,
  DB_USER: db_user,
  DB_PASSWORD: db_password,
  DB_NAME: db_name,
} = process.env;

// Helper: Get API token
async function getApiToken() {
  const headers = {
    "Ditat-Application-Role": "Login to TMS",
    "ditat-account-id": "agylogistics",
    "Authorization": "Basic aG9zaGVscDp3RkxIbTYub2th",
  };
  const { data } = await axios.post(gettingTokenUrl, {}, { headers });
  return data;
}

// Helper: Fetch driver data
async function fetchDrivers(apiToken) {
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
  const { data } = await axios.post(baseUrl, body, { headers });
  return data?.data?.data || [];
}

// Helper: Upsert drivers into DB
async function upsertDrivers(drivers) {
  const connection = await mysql.createConnection({
    host: db_host,
    user: db_user,
    password: db_password,
    database: db_name,
  });

  const sql = `
    INSERT INTO drivers (driverId, driver_data)
    VALUES (?, ?)
    ON DUPLICATE KEY UPDATE driver_data = VALUES(driver_data)
  `;

  try {
    for (const driver of drivers) {
      const driverId = driver.driverId;
      const driverJson = JSON.stringify(driver);
      await connection.execute(sql, [driverId, driverJson]);
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

    const drivers = rawDrivers.map((d) => ({
      driverId: d.driverId,
      Status: d.Status,
      firstName: d.firstName,
      lastName: d.lastName,
      truckId: d.truckId,
      phoneNumber: d.phoneNumber,
      emailAddress: d.emailAddress,
      hiredOn: d.hiredOn,
      updatedOn: d.updatedOn,
      companyId: d.companyId,
    }));

    await upsertDrivers(drivers);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error:`, error.message);
  }
}

// Run once at startup, then every 6 hours
fetchAndUpsertDrivers();
setInterval(fetchAndUpsertDrivers, 6 * 60 * 60 * 1000); // 6 hours

