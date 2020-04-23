const logger = require('./logger');
const ibmDb = require('ibm_db');

class Db2ConnectionPoolManager {
  connString;
  maxPoolConnections;
  pool;
  // Used to track the state of a pool start / restart
  isInitializing = true;
  isAvailable = false;

  constructor() {
    this.connString = this.createConnectionString();
    this.maxPoolConnections = 6;
    this.initializePool();
  }

  createConnectionString() {
    const databaseName = 'testdb';
    const hostname = '127.0.0.1';
    const port = 50000;
    const protocol = 'TCPIP';
    const username = 'db2inst1';
    const password = 'myibmdb2password';

    return `DATABASE=${databaseName};HOSTNAME=${hostname};PORT=${port};PROTOCOL=${protocol};UID=${username};PWD=${password}`;
  }

  /**
   * Initialize the pool with the number of _maxPoolConnections_.
   */
  initializePool() {
    logger.debug('Pool setup - initializing');
    this.isInitializing = true;

    if (!this.pool) {
      this.pool = new ibmDb.Pool();
      this.pool.setMaxPoolSize(this.maxPoolConnections);
    }

    const poolResult = this.pool.init(this.maxPoolConnections, this.connString);

    if (poolResult !== true) {
      this.isAvailable = false;
      logger.error(`Pool setup - ${poolResult.name}: ${poolResult.message}`);
    } else {
      this.isAvailable = true;
      logger.info(`Pool (${this.maxPoolConnections}) is up`);
    }
    this.isInitializing = false;
  }

  /**
   * Close existing pool connections if they exist, to prepare for reinitialization.
   */
  closePoolConnections() {
    return new Promise((resolve, reject) => {
      // Try-catch in the case that pool is undefined etc
      try {
        if (this.pool && this.pool.availablePool[this.connString]) {
          const availableConns = this.pool.availablePool[this.connString];

          for (let conn of availableConns) {
            conn.realClose((err) => {
              if (err) {
                logger.warn(`Pool refresh - ${err.name}: ${err.message}`);
              } else {
                logger.debug('Pool refresh - closed conn');
              }
            });
          }
        }
      } catch (err) {
        logger.warn(`Pool refresh - ${err.name}: ${err.message}`);
      }
      resolve();
    });
  }

  /**
   * Prevents further pool connections, closes the current pool and reinitializes it.
   */
  handlePoolRestart() {
    this.isInitializing = true;

    this.closePoolConnections()
      .then(() => {
        this.pool.availablePool[this.connString] = null;
        logger.info('Pool cleanup - complete');
        this.initializePool();
      })
      .catch((err) => {
        logger.warn(`Pool cleanup - ${err.name}: ${err.message}`);
      });
  }

  /**
   * Checks whether the pool has any connections currently in use.
   */
  hasActiveConnections() {
    try {
      return this.pool.usedPool[this.connString].length !== 0;
    } catch (err) {
      return false;
    }
  }

  getConnectionCounts() {
    let usedCount = 0;
    let availableCount = 0;
    let totalCount = 0;

    try {
      const usedCount = this.pool.usedPool[this.connString].length;
      const availableCount = this.pool.availablePool[this.connString].length;
      return {
        used: usedCount,
        available: availableCount,
        total: usedCount + availableCount,
      };
    } catch (err) {
      return { used: usedCount, available: availableCount, total: totalCount };
    }
  }

  /**
   * Attempts to get a good connection from the pool.
   * If this fails, it schedules a restart of the pool,
   * and returns a non-pool/direct connection as a fallback.
   */
  getConnection() {
    return new Promise(async (resolve, reject) => {
      try {
        logger.debug('Trying to get a pool connection');
        const conn = await this.getPoolConnection();
        logger.info('Got a connection from the pool');

        return resolve(conn);
      } catch (err) {
        logger.warn(`Pool connection err - ${err.name}: ${err.message}`);
        try {
          logger.debug('Fallback to direct connection');

          if (
            !this.isAvailable ||
            (!this.isInitializing && !this.hasActiveConnections())
          ) {
            logger.warn(`Pool has stale connections - triggering restart`);
            this.handlePoolRestart();
          }
          const conn = await this.getDirectConnection();
          logger.info('Got a connection directly');

          return resolve(conn);
        } catch (error) {
          return reject(error);
        }
      }
    });
  }

  /**
   * Attempts to get a connection from the pool.
   * It then tests the connectivity and returns it.
   */
  getPoolConnection() {
    return new Promise((resolve, reject) => {
      // Don't even try to get a connection if pool is restarting or otherwise unavailable
      if (this.isInitializing) {
        return reject(new Error('Pool is still initializing.'));
      }

      if (!this.isAvailable) {
        return reject(new Error('Pool is unavailable.'));
      }

      this.pool.open(this.connString, (err, conn) => {
        if (err) {
          try {
            // Even if err, the connection is now considered open, so close it
            conn.close(() => {
              return reject(err);
            });
          } catch (closeErr) {
            return reject(closeErr);
          }
        }

        // This query is an acid test of whether it's still active.
        conn.query('SELECT 1 from SYSIBM.SYSDUMMY1', (error, result) => {
          if (error) {
            try {
              // Even if err, the pool connection is still considered open, so close it
              conn.close(() => {
                return reject(error);
              });
            } catch (queryErr) {
              return reject(queryErr);
            }
          }

          // A stale connection will either return an error OR an empty result set.
          // If we have [ { '1': 1 } ] as a result, we know the connection is good
          if (result && result.length === 1) {
            return resolve(conn);
          } else {
            return reject(new Error('Connection test failed'));
          }
        });
      });
    });
  }

  /**
   * Fallback method - attempts to get a connection directly,
   * if the pool is unavailable.
   */
  getDirectConnection() {
    return new Promise((resolve, reject) => {
      ibmDb.open(this.connString, (err, conn) => {
        if (err) {
          reject(err);
        } else {
          resolve(conn);
        }
      });
    });
  }
}

const poolManager = new Db2ConnectionPoolManager();

module.exports = poolManager;
