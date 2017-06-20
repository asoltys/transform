function list(val) {
  return val.split(',');
}

const program = require('commander')
program
  .version('0.1.0')
  .usage('[options] <file>')
  .arguments('<file>')
  .option('-e, --empty [value]', 'Empty value [<NOVAL>]', '<NOVAL>')
  .option('-p, --params <params>', 'List of parameters', list, 'BATVOLT,SPCOND,ODO,PH,EXOTEMP,TURBID,W. LVL,EXOTEMP')
  .option('-y, --pretty <params>', 'Pretty names of parameters', list, 'Battery Voltage,SpCond,DO,pH,Water temp YSI,Turbidity,Stage,Water Temp PLS')
  .parse(process.argv)

const fs = require('fs')
const parser = require('csv-parse')({
  relax_column_count: true
})
const transform = require('stream-transform')
const input = fs.createReadStream(__dirname + '/' + program.args[0])
const output = {}
const params = []

const transformer = transform((data, callback) => {
  let keys = Object.keys(data)
  let rows = data.join(",") + '\n'

  let date = new Date(data[0])
  let time = new Date(data[0] + ' ' + data[1])
  let param = data[2]
  let value = data[3]

  if (!isNaN(date)) {
    hours = time.getHours()
    minutes = time.getMinutes()
    time = time.toTimeString().substr(0,5)
    
    if (minutes % 15 == 0) {
      if (!output[time]) output[time] = {}
      if (!params.includes(param) && (!program.params || program.params.includes(param))) {
        params.push(param)
      }

      output[time][param] = value
    }
  }
}, { parallel: 500, consume: true })

input
  .pipe(parser)
  .pipe(transformer)

transformer.on('finish', () => { 
  console.log(output)
  Object.keys(output).forEach((time) => {
    row = time + ','
    params.forEach((param) => {
      let v = output[time][param]
      if (!v) v = program.empty
      row += v + ','
    })
    console.log(row.slice(0,-1))
  })
})
