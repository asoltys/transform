const fs = require('fs')
const parser = require('csv-parse')({
  relax_column_count: true
})
const transform = require('stream-transform')
const input = fs.createReadStream(__dirname + '/apc.csv')

const transformer = transform((data, callback) => {
  let keys = Object.keys(data)
  let rows = data.join(",") + '\n'
  let date = new Date(data[0])
  if (!isNaN(date)) {
    callback(null, rows)
  }
}, { parallel: 500 })

input
  .pipe(parser)
  .pipe(transformer)
  .pipe(process.stdout)
