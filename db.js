import duckdb from 'duckdb';

let dbInstance = null;
let connection = null;
let tablePromise = null;

export async function getConnection() {
  if (!dbInstance) {
    dbInstance = new duckdb.Database(':memory:');
  }

  if (!connection) {
    connection = dbInstance.connect();
  }

  return connection;
}

export async function loadParquetOnce(conn) {
  if (!tablePromise) {
    tablePromise = new Promise((resolve, reject) => {
      conn.run(`
        CREATE TABLE IF NOT EXISTS properties AS 
        SELECT * FROM read_parquet('https://pub-465091b295bd4eceb75d79e289a45c27.r2.dev/properties_final.parquet')
      `, (err) => {
        if (err) {
          tablePromise = null;
          reject(err);
        } else {
          console.log('✅ Parquet loaded once');
          resolve(true);
        }
      });
    });
  }

  return tablePromise;
}