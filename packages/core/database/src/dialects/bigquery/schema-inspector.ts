// src/bigquery-schema-inspector.ts
import { Knex } from 'knex';
import type { Database } from '../..';
import type { Schema, Column, Index, ForeignKey } from '../../schema/types';
import type { SchemaInspector } from '../dialect';

interface RawTable {
  table_name: string;
}

interface RawColumn {
  data_type: string;
  column_name: string;
  character_maximum_length: number;
  column_default: string;
  is_nullable: string;
}

const toStrapiType = (column: RawColumn) => {
  const rootType = column.data_type.toLowerCase().match(/[^(), ]+/)?.[0];

  switch (rootType) {
    case 'integer': {
      // find a way to figure out the increments
      return { type: 'integer' };
    }
    case 'text': {
      return { type: 'text', args: ['longtext'] };
    }
    case 'boolean': {
      return { type: 'boolean' };
    }
    case 'character': {
      return { type: 'string', args: [column.character_maximum_length] };
    }
    case 'timestamp': {
      return { type: 'datetime', args: [{ useTz: false, precision: 6 }] };
    }
    case 'date': {
      return { type: 'date' };
    }
    case 'time': {
      return { type: 'time', args: [{ precision: 3 }] };
    }
    case 'numeric': {
      return { type: 'decimal', args: [10, 2] };
    }
    case 'real':
    case 'double': {
      return { type: 'double' };
    }
    case 'bigint': {
      return { type: 'bigInteger' };
    }
    case 'jsonb': {
      return { type: 'jsonb' };
    }
    default: {
      return { type: 'specificType', args: [column.data_type] };
    }
  }
};

export default class BigQuerySchemaInspector implements SchemaInspector {
  db: Database;
  
  bigQueryClient: Knex;

  constructor(db: Database, bigQueryClient: Knex) {
    this.db = db;
    this.bigQueryClient = bigQueryClient;
  }

  async getSchema(): Promise<Schema> {
    const schema: Schema = { tables: [] };

    const tables = await this.getTables();

    schema.tables = await Promise.all(
      tables.map(async (tableName) => {
        const columns = await this.getColumns(tableName);
        const indexes = await this.getIndexes(tableName);
        const foreignKeys = await this.getForeignKeys(tableName);

        return {
          name: tableName,
          columns,
          indexes,
          foreignKeys,
        };
      })
    );

    return schema;
  }

  getDatabaseSchema(): string {
    return this.db.getSchemaName() || 'public';
  }

  async getTables(): Promise<string[]> {
    const tables = await this.bigQueryClient('information_schema.tables')
      .where('table_schema', this.getDatabaseSchema())
      .where('table_type', 'BASE TABLE')
      .whereNot('table_name', 'geometry_columns')
      .whereNot('table_name', 'spatial_ref_sys')
      .select('table_name');

    return tables.map((table: RawTable) => table.table_name);
  }

  async getColumns(tableName: string): Promise<Column[]> {
    const columns = await this.bigQueryClient('information_schema.columns')
      .where('table_schema', this.getDatabaseSchema())
      .where('table_name', tableName)
      .select('data_type', 'column_name', 'character_maximum_length', 'column_default', 'is_nullable');

    return columns.map((row: RawColumn) => {
      const { type, args = [], ...rest } = toStrapiType(row);

      const defaultTo =
        row.column_default && row.column_default.includes('nextval(') ? null : row.column_default;

      return {
        type,
        args,
        defaultTo,
        name: row.column_name,
        notNullable: row.is_nullable === 'NO',
        unsigned: false,
        ...rest,
      };
    });
  }

  async getIndexes(tableName: string): Promise<Index[]> {
    // BigQuery does not support traditional indexes like other SQL databases
    return [];
  }

  async getForeignKeys(tableName: string): Promise<ForeignKey[]> {
    // BigQuery does not support foreign keys in the same way as traditional SQL databases
    return [];
  }
}
