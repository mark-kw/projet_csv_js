import fs from 'fs';

export async function processLineByLine() {
    const fileStream = fs.createReadStream('/Users/markkissi/final.csv');
    console.time("monTimer");
    var i = 0;
    var tab = [];
    fileStream.on('readable', () => {
        var buffer;
        while ((buffer = fileStream.read(10000000)) !== null) {
            var name = 'mynewcsv' + i + '.csv';
            fs.appendFile('./my_csv/' + name, buffer, function(err) {
                if (err) throw err;
            });
            i++;
            tab.push(name);
        }

        //console.log(tab);


    })
    fileStream.on('end', () => {
        console.timeEnd('monTimer')
    })
    return tab;
}

//console.log(processLineByLine());
// module.exports = { processLineByLine };