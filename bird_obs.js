const headers = '"Survey_Number","Record_Name","Date_Created","Status","ID","Survey_Type","Bird_Survey","Operator_name","Date","Observer_1","Observer_2","Observer_3","LIF_Name","Visibility","Surface_bitumen_present","Objects_covered_in_bitumen","Type_of_Observation","Bird_observation_locn_label","Track_starting_locn_label","single_location_easting","single_location_northing","track_start_easting","track_start_northing","location_based_easting","location_based_northing","Activity","Other_activity","Mode_of_transport","Other_mode_of_transport","Start_Time","Birds_Observed","Bird_Group_time","Distance_to_bird","Bearing_to_bird","bird_easting","bird_northing","Migrant_or_Seasonal_Resident","Species","Guild","Number_of_flightcapabale_birds","Number_of_Chicks","oiled_birds","Bird_Oiling_Level","Number_of_Birds_None","Number_of_birds_trace","Number_of_birds_light","Number_of_Birds_Moderate","Number_of_Birds_Heavy","Number_of_Birds_Complete","Bird_End_State","Was_hazing_conducted","No_hazing_reason","No_hazing_comments","Flushing_distance","Hazing_equipment","Hazing_equipment_other","Haze_attempt_success","Haze_successful_number","Haze_unsuccessful_number","Hazing_comments","Bird_observation_comments","End_Time","track_end_easting","track_end_northing","Distance_m","GPX_track_file_name","Hours_not_observing","Minutes_not_observing","Final_Comments","Form_Record","Mobile_Device","Nickname","Edited_By","Edit_Date"'

console.log(headers)

const fs = require('fs')
const parser = require('csv-parse')({
  columns: true
})
const transform = require('stream-transform')

const input = fs.createReadStream(__dirname + '/bird.csv')

const writer = transform((data, callback) => {
  let index = headers.split(',').indexOf('"Operator_name"')
  let filename = __dirname + '/' + data.split(',')[index].replace(/"/g,'')  + '.csv'
  let w = fs.createWriteStream(filename, { flags: 'a' })
  w.on('open', fd => {
    w.write(data)
  })
  callback(null, data)
})

const transformer = transform((data, callback) => {
  let keys = Object.keys(data)

  let re1 = /time\(1\)/g
  let re2 = /\(2\)/g
  let re3 = /,2\)/g
  let re4 = /,3\)/g

  let first = keys.findIndex(e => e.match(re1))
  let second = keys.findIndex(e => e.match(re2))
  let first_sub = keys.findIndex(e => e.match(re3))
  let second_sub = keys.findIndex(e => e.match(re4))

  let counter = 1

  let length = second - first;
  let sub_length = second_sub - first_sub;

  let rows = ''

  for (let i = 0; i < 20; i++) {
    let clone = Object.assign({}, data)

    hit = false
    start = first + i * length
    end = start + length - 1
    clone[keys]

    for (let j = start; j < end; j++) {
      if (data[keys[j]]) {
        hit = true
        break
      }
    }

    if (hit || i == 0) {
      for (j = first; j < first + length; j++) {
        clone[keys[j]] = data[keys[(start - first) + j]]
      }

      for (j = second; j < second + 19 * length; j++) {
        delete clone[keys[j]]
      }

      let clone2 = Object.assign({}, clone)

      for (let k = 0; k < 2; k++) {
        hit = false
        start = first_sub + k * sub_length
        end = start + sub_length
        
        for (j = start; j < end; j++) {
          let val = clone[keys[j]]
          clone2[keys[j - sub_length * (k + 1)]] = val
          delete clone[keys[j]]
          delete clone2[keys[j]]
          if (val) {
            hit = true
          }
        }

        if (hit) {
          rows += '"' + (parseInt(i) + 1) + '","' + Object.values(clone2).join('","') + '"\n'
        }
      }

      rows = '"' + (parseInt(i) + 1) + '","' + Object.values(clone).join('","') + '"\n' + rows

    }
  }

  callback(null, rows)
}, { parallel: 500 })

input
  .pipe(parser)
  .pipe(transformer)
  .pipe(writer)
  .pipe(process.stdout)
