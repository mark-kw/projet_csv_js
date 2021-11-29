import pm2 from 'pm2';
import { processLineByLine } from './files.js'


pm2.connect((err) => {
    if (err) {
        console.error(err)
        process.exit(2)
    }

    const nbfile = Math.floor(93 / 4);
    const rest = 93 - nbfile * 4;

    const arr = []
    for (var i = 0; i < 93; i++) {
        arr.push('mynewcsv' + i + '.csv');
    }
    //const arr = await processLineByLine();
    //console.log(arr);
    // const arr = ["cvs-1"]

    for (let i = 0; i < 4; i += 1) {
        var tab_arg = arr.slice(i * nbfile, i * nbfile + nbfile);
        const args = [JSON.stringify(tab_arg)];
        //console.log(args);
        pm2.start({
            script: './index.js',
            name: 'test' + i,
            args: args,
            autorestart: false
        }, (err, apps) => {
            console.log(apps);
        });
    }
});