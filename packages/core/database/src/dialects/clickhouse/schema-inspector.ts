// src/clickhouse-schema-inspector.ts
import { ClickHouseClient } from '@clickhouse/client';
import type { Database } from '../..';
import type { Schema, Column, Index, ForeignKey } from '../../schema/types';
import type { SchemaInspector } from '../dialect';

interface RawTable {
  name: string;
}

interface RawColumn {
  data_type: string;
  name: string;
  default_expression: string;
  is_nullable: number;
}

const toStrapiType = (column: RawColumn) => {
  const rootType = column.data_type.toLowerCase().match(/[^(), ]+/)?.[0];

  switch (rootType) {
    case 'int':
    case 'int32':
    case 'int64': {
      return { type: 'integer' };
    }
    case 'text': {
      return { type: 'text', args: ['longtext'] };
    }
    case 'bool': {
      return { type: 'boolean' };
    }
    case 'varchar': {
      return { type: 'string', args: [column.default_expression] };
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
    case 'decimal': {
      return { type: 'decimal', args: [10, 2] };
    }
    case 'float':
    case 'double': {
      return { type: 'double' };
    }
    case 'bigint': {
      return { type: 'bigInteger' };
    }
    case 'json': {
      return { type: 'jsonb' };
    }
    default: {
      return { type: 'specificType', args: [column.data_type] };
    }
  }
};

export default class ClickhouseSchemaInspector implements SchemaInspector {
  db: Database;

  clickhouseClient: ClickHouseClient;

  constructor(db: Database, clickhouseClient: ClickHouseClient) {
    this.db = db;
    this.clickhouseClient = clickhouseClient;
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
    return this.db.getSchemaName() || 'default';
  }

  async getTables(): Promise<string[]> {
    const tables = await this.clickhouseClient.query({
      query: `SHOW TABLES FROM ${this.getDatabaseSchema()}`,
      format: 'JSONEachRow',
    });

    const data = await tables.json() as RawTable[];

    return data.map((row: RawTable) => row.name);
  }

  async getColumns(tableName: string): Promise<Column[]> {
    const columns = await this.clickhouseClient.query({
      query: `DESCRIBE TABLE ${this.getDatabaseSchema()}.${tableName}`,
      format: 'JSONEachRow',
    });

    const data = await columns.json() as RawColumn[];

    return data.map((row: RawColumn) => {
      const { type, args = [], ...rest } = toStrapiType(row);

      const defaultTo =
        row.default_expression && row.default_expression.includes('nextval(')
          ? null
          : row.default_expression;

      return {
        type,
        args,
        defaultTo,
        name: row.name,
        notNullable: row.is_nullable === 0,
        unsigned: false,
        ...rest,
      };
    });
  }

  async getIndexes(tableName: string): Promise<Index[]> {
    // ClickHouse does not support traditional indexes like other SQL databases
    return [];
  }

  async getForeignKeys(tableName: string): Promise<ForeignKey[]> {
    // ClickHouse does not support foreign keys in the same way as traditional SQL databases
    return [];
  }
}
