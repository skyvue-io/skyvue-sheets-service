const knex = require('../../utils/knex');

const createTableFromColumns = (tableName, columns) =>
  knex.schema
    .createTableIfNotExists(tableName, table => {
      table.string('_id');
      columns.forEach(col => {
        table.string(col._id);
      });
    })
    .raw(
      `
          alter table "${tableName}" drop constraint if exists "${tableName}__id_unique";
          alter table "${tableName}" add constraint "${tableName}__id_unique" unique ("_id");
        `,
    )
    .toString();

module.exports = createTableFromColumns;
