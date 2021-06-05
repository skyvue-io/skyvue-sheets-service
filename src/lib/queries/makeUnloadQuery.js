const makeUnloadQuery = (destination, selectQuery) =>
  `
    UNLOAD ('${selectQuery}')
    TO '${destination}'
    iam_role '${process.env.REDSHIFT_IAM_ROLE}'
    ALLOWOVERWRITE
    format as CSV
  `;

module.exports = makeUnloadQuery;
