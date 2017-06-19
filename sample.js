const fs = require('fs')
const parser = require('csv-parse')({
  columns: true
})
const transform = require('stream-transform')

const input = fs.createReadStream(__dirname + '/bird.csv')

const transformer = transform((data, callback) => {
  let re1 = /\((1)\)/g
  let re2 = /\((2)\)/g
  let counter = 1
  let r = ''

  let keys = Object.keys(data)
  let first = keys.findIndex(e => e.match(re1))
  let second = keys.findIndex(e => e.match(re2))
  let length = second - first;

  let hit = false

  for (let i = 0; i < 20; i++) {
    let clone = Object.assign({}, data)
    hit = false
    start = first + i * length
    end = start + length

    for (let j = start; j < end; j++) {
      if (data[keys[j]]) {
        hit = true
      }
    }

    if (hit) {
      for (j = first; j < second; j++) {
        clone[keys[j]] = data[keys[start + j]]
      }

      for (j = second; j < start + 20 * length; j++) {
        delete clone[keys[j]]
      }

      r += Object.values(clone).join(',')+'\n'
    }
  }

  callback(null, r)
}, { parallel: 500 })

input
  .pipe(parser)
  .pipe(transformer)
  .pipe(process.stdout)
