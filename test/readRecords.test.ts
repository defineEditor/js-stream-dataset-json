import DatasetJson from '../src/index';
import { ItemDataObject } from '../src/interfaces/datasetJson';


test('Read the first and the last record dataset', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);

    const result: ItemDataObject[] = [];
    let rowsCount = 0;
    for await (const row of data.readRecords({type: 'object'}) as AsyncIterable<ItemDataObject>) {
        rowsCount++;

        if (rowsCount === 1 || rowsCount === data.metadata.records) {
            result.push(row);
        }
    }

    expect(result).toMatchSnapshot();
});


test('Read all records twice with buffer of 10', async () => {
    const filePath = 'test/data/adsl.json';
    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();
    const expectedCount = metadata.records;

    // First read
    let count1 = 0;
    for await (const obs of data.readRecords({ bufferLength: 10, type: 'object' })) {
        if (obs) {
            count1++;
        }
    }

    // Second read
    let count2 = 0;
    for await (const obs of data.readRecords({ bufferLength: 10, type: 'object' })) {
        if (obs) {
            count2++;
        }
    }

    expect(count1).toBe(expectedCount);
    expect(count2).toBe(expectedCount);
});