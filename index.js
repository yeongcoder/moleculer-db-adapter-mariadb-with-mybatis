/*eslint-disable */
const _ = require("lodash");
const { ServiceSchemaError } = require("moleculer").Errors;
const mariadb = require("mariadb");
const mybatisMapper = require("mybatis-mapper");

class MariaAdapter {
  constructor(opts) {
    this.opts = opts;
    this.mapper = _.cloneDeep(mybatisMapper);
    this.pool = mariadb.createPool(this.opts);
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

  connect() {
    this.mapper.createMapper([this.service.schema.settings.mapperDir]);
    return this.pool.getConnection().then((conn) => {
      this.db = conn;
      this.db.queryExecute = this.queryExecute.bind(this);
      this.service.logger.info("MariaDB adapter has connected successfully.");
    });
  }

  disconnect() {
    return this.pool.end();
  }

  queryExecute(id, params) {
    console.log(this.mapper);
    const sql = this.mapper.getStatement("mariadb", id, params);
    this.service.logger.info(sql);
    return this.db.query(sql);
  }
}

module.exports = MariaAdapter;
