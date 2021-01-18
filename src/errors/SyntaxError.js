class SyntaxError extends Error {
  constructor(_id, ...params) {
    super(...params);
    this.message = 'Syntax Error';
    this._id = _id;
    this.type = 'syntax_error';
  }
}

module.exports = SyntaxError;
