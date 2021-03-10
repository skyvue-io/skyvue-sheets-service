class SyntaxError extends Error {
  constructor(_id, message, ...params) {
    super(...params);
    this.message = message;
    this._id = _id;
    this.type = 'syntax_error';
  }
}

module.exports = SyntaxError;
