const fs = require('fs')
const util = require('util');
const fastcsv = require('fast-csv');
const xlsx = require('xlsx');
const tmp = require('tmp');

const createFile = util.promisify(fs.writeFile);
const rmdir = util.promisify(fs.rmdir);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

async function jobArrived(s, flowElement, job) {
    try {
        const jobPath = await job.get(AccessLevel.ReadOnly)

        const csvFolder = tmp.dirSync().name;

        const jobProperName = job.getName(false);

        const workbook = xlsx.readFile(jobPath);

        const writeOperations = workbook.SheetNames.map(sheetName => {
            return new Promise((resolve, reject) => {
                const worksheet = workbook.Sheets[sheetName];
                const rowCount = xlsx.utils.decode_range(worksheet['!ref']).e.r;
                const csvFilePath = `${csvFolder}/${sheetName}.csv`;
                const stream = fs.createWriteStream(csvFilePath);

                stream.on('finish', () => resolve({ csvFilePath, rowCount }));
                stream.on('error', reject);

                fastcsv.write(xlsx.utils.sheet_to_json(worksheet, { header: 1 }), { headers: false }).pipe(stream);
            });
        });

        const fileInfos = await Promise.all(writeOperations);


        fileInfos.sort((a, b) => a.rowCount - b.rowCount)
            .forEach((info, index) => {
                const rank = String(index + 1).padStart(3, '0');
                const newFileName = `${rank}_${info.rowCount}_${info.csvFilePath.split('/').reverse()[0]}`;
                const newFilePath = `${csvFolder}/${newFileName}`;
                fs.renameSync(info.csvFilePath, newFilePath);
            });


        const newJob = await job.createChild(csvFolder);
        await newJob.sendToSingle(`${jobProperName}_csv`);

        await rmdir(csvFolder, { recursive: true, force: true });

        await job.sendToNull();
    } catch (err) {
        job.fail('Something went wrong with the job %1, err : %2', [job.getName(), err.message || err]);
    }


}
