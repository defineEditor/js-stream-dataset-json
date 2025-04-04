import DatasetJson from '../src/index';

test('Get metadata', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();
    expect(metadata).toMatchSnapshot();
});


test('Get metadata when some attributes are after rows', async () => {
    const filePath = 'test/data/adslWrongAttributeOrder.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();
    expect(metadata).toMatchSnapshot();
});

test('Get metadata of NDJSON dataset', async () => {
    const filePath = 'test/data/adsl.ndjson';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();
    expect(metadata).toMatchSnapshot();
});