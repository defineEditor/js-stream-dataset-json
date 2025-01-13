import DatasetJson from '../src/index';
import Filter from 'js-array-filter';

test('Get filtered rows of dataset with simple "and" filter', async () => {
    const filePath = 'test/data/adsl.ndjson';

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
    const filePath = 'test/data/adsl.ndjson';

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

test('Get filtered rows of dataset with all types of operators', async () => {
    const filePath = 'test/data/adsl.ndjson';

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
        connectors: ['or', 'or', 'or', 'or', 'or', 'or', 'or', 'or', 'or', 'or', 'or', 'or']
    });
    const rows = await data.getData({
        start: 0,
        filter,
        filterColumns: ['USUBJID', 'SEX', 'AGE', 'RACE', 'TRT01P', 'DCDECOD', 'DSDECOD']
    });
    expect(rows.length).toEqual(254);
    expect(rows).toMatchSnapshot();
});