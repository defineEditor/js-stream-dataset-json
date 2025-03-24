import DatasetJson from '../src/index';

test('Get unique values of variables', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const values = await data.getUniqueValues({columns: ['USUbjid', 'trtedt', 'STUDYID'], limit: 20});
    expect(values).toMatchSnapshot();
});

test('Get unique values of variables and do not sort the result', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const values = await data.getUniqueValues({columns: ['USUbjid', 'trtedt', 'STUDYID'], limit: 20, bufferLength: 10, sort: false});
    expect(values).toMatchSnapshot();
});

test('Get unique values with counts', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const values = await data.getUniqueValues({columns: ['USUbjid', 'trtedt', 'STUDYID', 'BMIBL'],
        limit: 10, addCount: true, sort: false});
    expect(values).toMatchSnapshot();
});