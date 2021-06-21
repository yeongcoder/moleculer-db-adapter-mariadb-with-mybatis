/*eslint-disable */
const _ = require("lodash");
const { MoleculerServerError } = require("moleculer").Errors;
const mariadb = require("mariadb");
const mybatisMapper = require("mybatis-mapper");

class MariaDbAdapter {
  /**
   * Creates an instance of MariaDbAdapter.
   * @param {mariadb.PoolConfig} opts
   *
   * @memberof MariaDbAdapter
   */
  constructor(opts) {
    this.opts = opts;
    this.mapper = _.cloneDeep(mybatisMapper);
  }

  /**
   * Initialize adapter
   *
   * @param {ServiceBroker} broker
   * @param {Service} service
   *
   * @memberof MariaAdapter
   */
  init(broker, service) {
    this.broker = broker;
    this.service = service;
  }

  /**
   * Connect to database
   *
   * @returns {Promise}
   *
   * @memberof MariaDbAdapter
   */
  connect() {
    if (!this.service.schema.settings.mapperDir) {
      throw new MoleculerServerError(
        "Missing `mapperDir` definition in schema.settings of service!"
      );
    }
    this.pool = mariadb.createPool(this.opts);
    this.mapper.createMapper([this.service.schema.settings.mapperDir]);
    return this.pool.getConnection().then((conn) => {
      this.db = conn;
      this.db.sendQuery = this.sendQuery.bind(this);
      this.service.logger.info("MariaDB adapter has connected successfully.");
    });
  }

  /**
   * Disconnect from database
   *
   * @returns {Promise}
   *
   * @memberof MariaDbAdapter
   */
  disconnect() {
    if (this.pool) {
      this.pool.end();
    }
    return Promise.resolve();
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} id
   * @param {object?} params
   * @returns {Promise}
   *
   * @memberof MariaDbAdapter
   */
  sendQuery(id, params) {
    const sql = this.mapper.getStatement("mariadb", id, params);
    this.service.logger.info(sql);
    return this.db.query(sql);
  }
}

module.exports = MariaDbAdapter;
