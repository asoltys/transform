console.log('"Survey_Number","Record_Name","Date_Created","Status","ID","Survey_type","Mortality_Search","Operator_name","Date","Searcher_1","Searcher_2","Searcher_3","LIF_Name","Activity_During_Search","Activity_Comments","Type_of_Search","Search_Method","Other_search_method","Visible_Strip_Left_m","Visible_Strip_Right_m","Visible_Range_Fixed_radius","Left_Bearing","Right_Bearing","transect_start_locn_label","fixed_radius_locn_label","small_lif_locn_label","Easting_start","Northing_start","transect_start_easting","transect_start_northing","fixed_or_smallLIF_easting","fixed_or_smallLIF_northing","Start_Time","Bird_Found","Time_Bird_Found","Bird_location_available","Distance_to_Bird","Bearing_to_bird_degrees","bird_locn_label","bird_locn_notatbird_label","bird_Easting","bird_Northing","crew_locn_notatbird_easting","crew_locn_notatbird_northing","Species","Guild","Number_of_Birds","State_of_Bird_When_Found","Bird_Oiling_Level","End_State","Bird_Body_Condition","Transect_end_Easting","Transect_end_Northing","transect_distance_m","GPX_Track_File_Name","End_Time","General_comments","Form_Record","Mobile_Device","Nickname","Edited_By","Edit_Date"')

const fs = require('fs')
const parser = require('csv-parse')({
  columns: true
})
const transform = require('stream-transform')

const input = fs.createReadStream(__dirname + '/mortality.csv')

const transformer = transform((data, callback) => {
  let keys = Object.keys(data)
  let rows = ''

  let first = keys.findIndex(e => e.match(/\(1\)/g))
  let second = keys.findIndex(e => e.match(/\(2\)/g))
  let length = second - first;

  for (let i = 0; i < 20; i++) {
    let clone = Object.assign({}, data)

    hit = false
    start = first + i * length
    end = start + length - 1

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

      rows = '"' + (parseInt(i) + 1) + '","' + Object.values(clone).join('","') + '"\n' + rows
    }
  }

  callback(null, rows)
}, { parallel: 500 })

input
  .pipe(parser)
  .pipe(transformer)
  .pipe(process.stdout)
