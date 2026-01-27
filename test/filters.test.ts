import DatasetJson from '../src/index';
import Filter, { BasicFilter } from 'js-array-filter';

test('Get filtered rows of dataset with simple "and" filter', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'gt', value: 80 },
            { variable: 'SEX', operator: 'eq', value: 'M' }
        ],
        connectors: ['and']
    });
    const rows = await data.getData({ start: 0, length: 5, filter, filterColumns: ['USUBJID', 'SEX', 'AGE'] });
    expect(rows.length).toBeLessThanOrEqual(5);
    expect(rows).toMatchSnapshot();
});

test('Get filtered rows of dataset with simple "or" filter', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'gt', value: 85 },
            { variable: 'DCDECOD', operator: 'eq', value: 'STUDY TERMINATED BY SPONSOR' }
        ],
        connectors: ['or']
    });
    const rows = await data.getData({ start: 0, filter, filterColumns: ['USUBJID', 'DCDECOD', 'AGE'] });
    expect(rows.length).toEqual(24);
});

test('Get filtered rows of dataset with eq operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'DCDECOD', operator: 'eq', value: 'STUDY TERMINATED BY SPONSOR' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'DCDECOD']
    });
    expect(rows.length).toEqual(7);
});

test('Get filtered rows of dataset with contains operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'RACE', operator: 'contains', value: 'WHITE' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'RACE']
    });
    expect(rows.length).toEqual(230);
});

test('Get filtered rows of dataset with contains operator and case insensitive option', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'RACE', operator: 'contains', value: 'bLACK' }
        ],
        connectors: [],
        options: { caseInsensitive: true }
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'RACE']
    });
    expect(rows.length).toEqual(23);
});

test('Get filtered rows of dataset with notcontains operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'RACE', operator: 'notcontains', value: 'WHITE' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'RACE']
    });
    expect(rows.length).toEqual(24);
});

test('Get filtered rows of dataset with starts operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'TRT01P', operator: 'starts', value: 'Xanomeline Low' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'TRT01P']
    });
    expect(rows.length).toEqual(84);
});

test('Get filtered rows of dataset with ends operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'TRT01P', operator: 'ends', value: 'ebo' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'TRT01P']
    });
    expect(rows.length).toEqual(86);
});

test('Get filtered rows of dataset with regex operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'TRT01P', operator: 'regex', value: '^Xano.*Dose$' }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'TRT01P']
    });
    expect(rows.length).toEqual(168);
});

test('Get filtered rows of dataset with regex operator and case insensitive option', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'TRT01P', operator: 'regex', value: '^pLaCEBO$' }
        ],
        connectors: [],
        options: { caseInsensitive: true }
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'TRT01P']
    });
    expect(rows.length).toEqual(86);
});

test('Get filtered rows of dataset with in operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'USUBJID', operator: 'in', value: ['01-701-1015', '01-702-1082'] }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID']
    });
    expect(rows.length).toEqual(2);
});

test('Get filtered rows of dataset with notin operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'DCDECOD', operator: 'notin', value: ['ADVERSE EVENT', 'DEATH', 'COMPLETED'] }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'DCDECOD']
    });
    expect(rows.length).toEqual(49);
});

test('Get filtered rows of dataset with in operator and case insensitive option', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'DCDECOD', operator: 'in', value: ['ADVERSE event', 'DeAtH', 'ComplETED'] }
        ],
        connectors: [],
        options: { caseInsensitive: true }
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'DCDECOD']
    });
    expect(rows.length).toEqual(205);
});

test('Get filtered rows of dataset with gt operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'gt', value: 80 }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'AGE']
    });
    expect(rows.length).toEqual(77);
});

test('Get filtered rows of dataset with lt operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'lt', value: 53 }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'AGE']
    });
    expect(rows.length).toEqual(2);
});

test('Get filtered rows of dataset with ge operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'ge', value: 89 }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'AGE']
    });
    expect(rows.length).toEqual(1);
});

test('Get filtered rows of dataset with le operator', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'AGE', operator: 'le', value: 51 }
        ],
        connectors: []
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'AGE']
    });
    expect(rows.length).toEqual(1);
});

test('Get filtered rows of dataset with all types of operators', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);
    const metadata = await data.getMetadata();

    const filter = new Filter('dataset-json1.1', metadata.columns, {
        conditions: [
            { variable: 'DCDECOD', operator: 'eq', value: 'STUDY TERMINATED BY SPONSOR' },
            { variable: 'RACE', operator: 'contains', value: 'BL' },
            { variable: 'RACE', operator: 'notcontains', value: 'HISP' },
            { variable: 'TRT01P', operator: 'starts', value: 'P' },
            { variable: 'TRT01P', operator: 'ends', value: 'ebo' },
            { variable: 'TRT01P', operator: 'regex', value: '^Xano.*Low.*Dose$' },
            { variable: 'USUBJID', operator: 'in', value: ['01-701-1015', '01-702-1082'] },
            { variable: 'DCDECOD', operator: 'notin', value: ['ADVERSE EVENT', 'DEATH', 'COMPLETED'] },
            { variable: 'AGE', operator: 'in', value: [75, 76] },
            { variable: 'AGE', operator: 'gt', value: 80 },
            { variable: 'AGE', operator: 'lt', value: 55 },
            { variable: 'AGE', operator: 'ge', value: 82 },
            { variable: 'AGE', operator: 'le', value: 60 },
        ],
        connectors: ['or', 'or', 'and', 'or', 'and', 'or', 'or', 'and', 'or', 'and', 'or', 'or']
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'SEX', 'AGE', 'RACE', 'TRT01P', 'DCDECOD', 'DSDECOD']
    });
    expect(rows.length).toEqual(75);
    expect(rows).toMatchSnapshot();
});

test('Use BasicFilter', async () => {
    const filePath = 'test/data/adsl.json';

    const data = new DatasetJson(filePath);

    const filter =  {
        conditions: [
            { variable: 'AGE', operator: 'gt', value: 80 },
            { variable: 'SEX', operator: 'eq', value: 'M' }
        ],
        connectors: ['and']
    } as BasicFilter;
    const rows = await data.getData({ start: 0, length: 5, filter, filterColumns: ['USUBJID', 'SEX', 'AGE'] });
    expect(rows.length).toBeLessThanOrEqual(5);
    expect(rows).toMatchSnapshot();
});