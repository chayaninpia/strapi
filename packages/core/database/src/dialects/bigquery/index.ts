// src/bigquery-dialect.ts
import knex, { Knex } from 'knex';
import { Client } from 'knex-bigquery';
import * as errors from '../../errors';
import type { Database } from '../..';
import Dialect from '../dialect';
import BigQuerySchemaInspector from './schema-inspector';

export default class BigQueryDialect extends Dialect {
  schemaInspector: BigQuerySchemaInspector;

  bigQueryClient: Knex;

  constructor(db: Database) {
    super(db, 'bigquery');

    this.bigQueryClient = knex({
      client: Client,
      connection: db.config.connection,
    });

    this.schemaInspector = new BigQuerySchemaInspector(db, this.bigQueryClient);
  }

  useReturning() {
    return false;
  }

  async initialize() {
    // Initialize any BigQuery specific setup if necessary
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
