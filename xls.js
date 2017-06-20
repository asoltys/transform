const excel = require('exceljs')
const firstline = require('firstline')

const workbook = new excel.Workbook()
workbook.csv.readFile('data.csv').then(worksheet => {
  firstline('data.csv').then(line => {
    worksheet.columns = line.split(',').map(i => {
      return { header: i, key: i, width: 10 }
    })
  })

  worksheet.workbook.xlsx.writeFile('test.xlsx').then(function() {
    console.log('yeehaw')
  })
})
