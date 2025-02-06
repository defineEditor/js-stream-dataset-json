import { describe, it, expect, afterEach } from '@jest/globals';
import fs from 'fs';
import zlib from 'zlib';
import DatasetJson from '../src/class/datasetJson';
import { DatasetMetadata } from '../src/interfaces/datasetJson';

describe('DatasetJson write tests', () => {
    const testJsonPath = 'test/data/temp-test.json';
    const testNdjsonPath = 'test/data/temp-test.ndjson';
    const testCompressedPath = 'test/data/temp-test.dsjc';

    afterEach(() => {
        // Cleanup test files after each test
        [testJsonPath, testNdjsonPath, testCompressedPath].forEach(path => {
            if (fs.existsSync(path)) {
                fs.unlinkSync(path);
            }
        });
    });

    const testMetadata: DatasetMetadata = {
        datasetJSONCreationDateTime: '2024-01-01T00:00:00',
        datasetJSONVersion: '1.1',
        records: 3,
        name: 'TEST',
        label: 'Test Dataset',
        columns: [
            { name: 'USUBJID', label: 'Subject ID', dataType: 'string', itemOID: 'USUBJID' },
            { name: 'AGE', label: 'Age', dataType: 'integer', itemOID: 'AGE' }
        ]
    };

    const testData = [
        ['STUDY01-SITE01-SUBJ001', 25],
        ['STUDY01-SITE01-SUBJ002', 30],
        ['STUDY01-SITE02-SUBJ002', 30]
    ];

    describe('JSON format tests', () => {
        it('should write metadata and data correctly in JSON format', async () => {
            const dataset = new DatasetJson(testJsonPath);

            await dataset.writeData({
                metadata: testMetadata,
                data: testData
            });

            // Read and verify the written file
            const content = fs.readFileSync(testJsonPath, 'utf8');
            const parsed = JSON.parse(content);

            expect(parsed.datasetJSONVersion).toBe(testMetadata.datasetJSONVersion);
            expect(parsed.name).toBe(testMetadata.name);
            expect(parsed.rows).toEqual(testData);
        });

        it('should write prettified JSON when specified', async () => {
            const dataset = new DatasetJson(testJsonPath);

            await dataset.writeData({
                metadata: testMetadata,
                data: testData,
                options: { prettify: true }
            });

            const content = fs.readFileSync(testJsonPath, 'utf8');

            // Verify formatting
            expect(content).toContain('\n');
            expect(content).toContain('  ');

            // Verify content
            const parsed = JSON.parse(content);
            expect(parsed.rows).toEqual(testData);
        });

        it('should support incremental writes in JSON format', async () => {
            const dataset = new DatasetJson(testJsonPath);

            await dataset.write({ metadata: testMetadata, action: 'create' });
            await dataset.write({ data: [testData[0]], action: 'write' });
            await dataset.write({ data: testData.slice(1), action: 'write' });
            await dataset.write({ action: 'finalize' });

            const content = fs.readFileSync(testJsonPath, 'utf8');
            const parsed = JSON.parse(content);
            expect(parsed.rows).toEqual(testData);
        });
    });

    describe('NDJSON format tests', () => {
        it('should write metadata and data correctly in NDJSON format', async () => {
            const dataset = new DatasetJson(testNdjsonPath, { isNdJson: true });

            await dataset.writeData({
                metadata: testMetadata,
                data: testData
            });

            const lines = fs.readFileSync(testNdjsonPath, 'utf8').trim().split('\n');

            // Verify metadata line
            const parsedMetadata = JSON.parse(lines[0]);
            expect(parsedMetadata.datasetJSONVersion).toBe(testMetadata.datasetJSONVersion);

            // Verify data lines
            expect(JSON.parse(lines[1])).toEqual(testData[0]);
            expect(JSON.parse(lines[2])).toEqual(testData[1]);
        });

        it('should support incremental writes in NDJSON format', async () => {
            const dataset = new DatasetJson(testNdjsonPath, { isNdJson: true });

            await dataset.write({ metadata: testMetadata, action: 'create' });
            await dataset.write({ data: [testData[0]], action: 'write' });
            await dataset.write({ data: [testData[1]], action: 'write' });
            await dataset.write({ action: 'finalize' });

            const lines = fs.readFileSync(testNdjsonPath, 'utf8').trim().split('\n');
            expect(lines.length).toBe(3);
            expect(JSON.parse(lines[1])).toEqual(testData[0]);
            expect(JSON.parse(lines[2])).toEqual(testData[1]);
        });
    });

    describe('Compressed format tests', () => {
        it('should write metadata and data correctly in compressed format', async () => {
            const dataset = new DatasetJson(testCompressedPath, { isCompressed: true });

            await dataset.writeData({
                metadata: testMetadata,
                data: testData
            });

            // Read and verify the compressed file
            const compressedContent = fs.readFileSync(testCompressedPath);
            const decompressedContent = zlib.gunzipSync(compressedContent).toString();
            const lines = decompressedContent.trim().split('\n');

            // Verify metadata line
            const parsedMetadata = JSON.parse(lines[0]);
            expect(parsedMetadata.datasetJSONVersion).toBe(testMetadata.datasetJSONVersion);
            expect(parsedMetadata.name).toBe(testMetadata.name);

            // Verify data lines
            expect(JSON.parse(lines[1])).toEqual(testData[0]);
            expect(JSON.parse(lines[2])).toEqual(testData[1]);
            expect(JSON.parse(lines[3])).toEqual(testData[2]);
        });

        it('should support incremental writes in compressed format', async () => {
            const dataset = new DatasetJson(testCompressedPath, { isCompressed: true });

            await dataset.write({ metadata: testMetadata, action: 'create' });
            await dataset.write({ data: [testData[0]], action: 'write' });
            await dataset.write({ data: testData.slice(1), action: 'write' });
            await dataset.write({ action: 'finalize' });

            // Read and verify the compressed file
            const compressedContent = fs.readFileSync(testCompressedPath);
            const decompressedContent = zlib.gunzipSync(compressedContent).toString();
            const lines = decompressedContent.trim().split('\n');

            expect(lines.length).toBe(4); // metadata + 3 data lines
            expect(JSON.parse(lines[1])).toEqual(testData[0]);
            expect(JSON.parse(lines[2])).toEqual(testData[1]);
            expect(JSON.parse(lines[3])).toEqual(testData[2]);
        });

        it('should automatically detect compressed format by file extension', async () => {
            const dataset = new DatasetJson(testCompressedPath); // No explicit isCompressed option

            await dataset.writeData({
                metadata: testMetadata,
                data: testData
            });

            // Verify file is actually compressed
            const compressedContent = fs.readFileSync(testCompressedPath);
            expect(() => {
                zlib.gunzipSync(compressedContent);
            }).not.toThrow();
        });
        it('should read comressed format using getData method', async () => {
            const dataset = new DatasetJson(testCompressedPath);

            await dataset.writeData({
                metadata: testMetadata,
                data: testData
            });

            // Read and verify the written file
            const rows = await dataset.getData({start: 0});
            expect(rows.length).toBe(3); // 3 data lines
        });
    });

    describe('Error handling tests', () => {
        it('should throw error when writing data without creating first', async () => {
            const dataset = new DatasetJson(testJsonPath);

            await expect(dataset.write({
                data: testData,
                action: 'write'
            })).rejects.toThrow('No active write stream');
        });

        it('should throw error when creating without metadata', async () => {
            const dataset = new DatasetJson(testJsonPath);

            await expect(dataset.write({
                action: 'create'
            })).rejects.toThrow('Metadata is required');
        });
    });
});
