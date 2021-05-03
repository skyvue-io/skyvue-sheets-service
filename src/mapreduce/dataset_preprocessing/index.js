#!/usr/bin/env node
/* eslint-disable no-extend-native */

const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
});

// escape all control characters so that they are plain text in the output
String.prototype.escape = function () {
  return this.replace('n', '\n')
    .replace('', "'")
    .replace('"', '"')
    .replace('&', '&')
    .replace('r', '\r')
    .replace('t', '\t')
    .replace('b', '\b')
    .replace('f', '\f');
};

// function which extracts and re-emits the bits of the tweet we want, in a format which is suitable for Hadoop MapReduce
exports.extractTweetInfo = function (line) {
  obj = JSON.parse(line);

  obj.interactions.map(item => {
    const a = item.interaction.author;
    const dateComponents = item.interaction.created_at.split(' ');
    const d = [dateComponents[1], dateComponents[2], dateComponents[3]].join(' ');

    const interaction = {
      objectId: obj.id,
      hash: obj.hash,
      id: item.interaction.id,
      author_id: a.id,
      author_avatar: a.avatar,
      author_link: a.link,
      author_name: a.name,
      author_username: a.username,
      content: item.interaction.content.escape(),
      created_at: item.interaction.created_at,
      link: item.interaction.link,
      schema_version: item.interaction.schema.version,
      source: item.interaction.source,
    };

    process.stdout.write(`${d}t${JSON.stringify(interaction)}n`);

    return undefined;
  });
};

rl.on('line', line => {
  exports.extractTweetInfo(line);
});
