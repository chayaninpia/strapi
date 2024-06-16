// src/clickhouse-dialect.ts
import { createClient, ClickHouseClient } from '@clickhouse/client';

import * as errors from '../../errors';
import type { Database } from '../..';
import Dialect from '../dialect';
import ClickhouseSchemaInspector from './schema-inspector';

export default class ClickhouseDialect extends Dialect {
  schemaInspector: ClickhouseSchemaInspector;

  clickhouseClient: ClickHouseClient;

  constructor(db: Database) {
    super(db, 'clickhouse');

    this.clickhouseClient = createClient({
      host: db.config.connection.host,
      username: db.config.connection.user,
      password: db.config.connection.password,
      database: db.config.connection.database,
    });

    this.schemaInspector = new ClickhouseSchemaInspector(db, this.clickhouseClient);
  }

  useReturning() {
    return false;
  }

  async initialize() {
    // Initialize any ClickHouse specific setup if necessary
  }

  usesForeignKeys() {
    return false;
  }

  getSqlType(type: string) {
    switch (type) {
      case 'timestamp': {
        return 'datetime';
      }
      default: {
        return type;
      }
    }
  }

  transformErrors(error: NodeJS.ErrnoException) {
    switch (error.code) {
      case 'notFound': {
        throw new errors.NotNullError({
          column: 'column' in error ? `${error.column}` : undefined,
        });
      }
      default: {
        super.transformErrors(error);
      }
    }
  }
}
