import fs from 'fs';
import fsPromises from 'fs/promises';
import readline from 'readline';
import zlib from 'zlib';
import {
    ItemDataArray,
    ItemDataObject,
    DatasetMetadata,
    DataType,
    UniqueValues,
    MetadataAttributes,
    ParsedAttributes,
    JSONStreamParser,
} from './../interfaces/datasetJson';
import JSONStream from 'JSONStream';
import Filter from 'js-array-filter';

// Main class for dataset JSON;
class DatasetJson {
    // Path to the file;
    filePath: string;
    // Statistics about the file;
    stats: fs.Stats | null;
    // Item Group metadata;
    metadata: DatasetMetadata;
    // Current position in the file;
    currentPosition: number;
    // Flag to indicate if all rows are read;
    allRowsRead: boolean;
    // Metadata loaded
    private metadataLoaded: boolean;
    // Stream
    private stream: fs.ReadStream | zlib.Gunzip | null;
    // Parser
    private parser?: JSONStreamParser;
    // Encoding
    private encoding: BufferEncoding;
    // NDJSON flag
    private isNdJson: boolean;
    // Compressed Dataset-JSON flag
    private isCompressed: boolean;
    // Read line stream for NDJSON
    private rlStream?: readline.Interface;
    // Required attributes
    private requiredAttributes = [
        'datasetJSONCreationDateTime',
        'datasetJSONVersion',
        'records',
        'name',
        'label',
        'columns',
    ];

    // Write stream for output
    private writeStream?: fs.WriteStream | zlib.Gzip;
    // Write mode
    private writeMode?: 'json' | 'ndjson';
    // First write flag
    private isFirstWrite: boolean;

    // Write queue management
    private writeQueueDrain: Promise<void> = Promise.resolve();

    /**
     * Read observations.
     * @constructor
     * @param filePath - Path to the file.
     * @param options - Configuration options
     * @param options.encoding - File encoding. Default is 'utf8'.
     * @param options.isNdJson - Force NDJSON format. If not specified, detected from file extension.
     * @param options.isCompressed - Force NDJSON format. If not specified, detected from file extension.
     * @param options.checkExists - Throw error if file does not exist. Default is false.
     */
    constructor(
        filePath: string,
        options?: {
            encoding?: BufferEncoding;
            isNdJson?: boolean;
            checkExists?: boolean;
            isCompressed?: boolean;
        }
    ) {
        this.filePath = filePath;
        this.currentPosition = 0;
        const { encoding = 'utf8', checkExists = false } = options || {};
        this.encoding = encoding;
        this.isFirstWrite = true;
        // If option isNdjson is not specified, try to detect it from the file extension;
        if (options?.isNdJson === undefined) {
            this.isNdJson = this.filePath.toLowerCase().endsWith('.ndjson');
        } else {
            this.isNdJson = options.isNdJson;
        }
        // If option isCompressed is not specified, try to detect it from the file extension;
        if (options?.isCompressed === undefined) {
            this.isCompressed = this.filePath.toLowerCase().endsWith('.dsjc');
        } else {
            this.isCompressed = options.isCompressed;
        }
        // In case of compressed file, change the NDJSON format is used
        if (this.isCompressed) {
            this.isNdJson = true;
        }

        this.allRowsRead = false;
        this.metadataLoaded = false;

        this.metadata = {
            datasetJSONCreationDateTime: '',
            datasetJSONVersion: '',
            records: -1,
            name: '',
            label: '',
            columns: [],
        };

        // Get all possible encoding values from BufferEncoding type
        const validEncodings: BufferEncoding[] = [
            'ascii',
            'utf8',
            'utf16le',
            'ucs2',
            'base64',
            'latin1',
        ];

        // Check encoding
        if (!validEncodings.includes(this.encoding)) {
            throw new Error(`Unsupported encoding ${this.encoding}`);
        }

        // Check if file exists;
        if (!fs.existsSync(this.filePath)) {
            if (checkExists === true) {
                throw new Error(`Could not read file ${this.filePath}`);
            } else {
                this.stats = null;
                this.stream = null;
            }
        } else {
            this.stats = fs.statSync(this.filePath);

            if (this.isCompressed) {
                const rawStream = fs.createReadStream(this.filePath);
                const gunzip = zlib.createGunzip();
                this.stream = rawStream.pipe(gunzip);
            } else {
                this.stream = fs.createReadStream(this.filePath, {
                    encoding: this.encoding,
                });
            }
        }
    }

    /**
     * Check if the file was modified
     * @return True if file has changed, otherwise false.
     */
    private async fileChanged(): Promise<boolean> {
        const stats = await fsPromises.stat(this.filePath);
        if (this.stats !== null && stats.mtimeMs !== this.stats.mtimeMs) {
            return true;
        }
        return false;
    }

    /**
     * Auxilary function to verify if required elements are parsed;
     * @return True if all required attributes are present, otherwise false.
     */
    private checkAttributesParsed = (item: {
        [name: string]: boolean;
    }): boolean => {
        return this.requiredAttributes.every((key) => item[key] === true);
    };

    /**
     * Get Dataset-JSON metadata
     * @return An object with file metadata.
     */
    async getMetadata(): Promise<DatasetMetadata> {
        // If the file did not change, use the metadata obtained during initialization;
        if (!(await this.fileChanged()) && this.metadataLoaded === true) {
            return this.metadata;
        } else {
            if (this.isNdJson) {
                return this.getNdjsonMetadata();
            } else {
                return this.getJsonMetadata();
            }
        }
    }

    /**
     * Get Dataset-JSON metadata when the file is in JSON format.
     * @return An object with file metadata.
     */
    private async getJsonMetadata(): Promise<DatasetMetadata> {
        return new Promise((resolve, reject) => {
            this.metadataLoaded = false;
            // Metadata for ItemGroup
            const metadata: DatasetMetadata = {
                datasetJSONCreationDateTime: '',
                datasetJSONVersion: '',
                records: -1,
                name: '',
                label: '',
                columns: [],
                studyOID: '',
                metaDataVersionOID: '',
            };
            const parsedMetadata: ParsedAttributes = {
                datasetJSONCreationDateTime: false,
                datasetJSONVersion: false,
                dbLastModifiedDateTime: false,
                fileOID: false,
                originator: false,
                sourceSystem: false,
                itemGroupOID: false,
                columns: false,
                records: false,
                name: false,
                label: false,
                studyOID: false,
                metaDataVersionOID: false,
                metaDataRef: false,
            };

            // Restart stream
            if (
                this.currentPosition !== 0 ||
                this.stream?.destroyed ||
                this.stream === null
            ) {
                if (this.stream !== null && !this.stream?.destroyed) {
                    this.stream?.destroy();
                }
                this.stream = fs.createReadStream(this.filePath, {
                    encoding: this.encoding,
                });
            }

            if (this.stream === null) {
                reject(
                    new Error(
                        'Could not create read stream for file ' + this.filePath
                    )
                );
                return;
            }

            this.stream
                .pipe(
                    JSONStream.parse(
                        'rows..*',
                        (data: string, nodePath: string) => {
                            return { path: nodePath, value: data };
                        }
                    )
                )
                .on('end', () => {
                    // Check if all required attributes are parsed after the file is fully loaded;
                    if (!this.checkAttributesParsed(parsedMetadata)) {
                        const notParsed = Object.keys(parsedMetadata).filter(
                            (key) =>
                                !parsedMetadata[key as MetadataAttributes] &&
                                this.requiredAttributes.includes(key)
                        );
                        reject(
                            new Error(
                                'Could not find required metadata elements ' +
                                    notParsed.join(', ')
                            )
                        );
                    }
                    this.metadataLoaded = true;
                    this.metadata = metadata;
                    resolve(metadata);
                })
                .on('header', (data: DatasetMetadata) => {
                    // In correctly formed Dataset-JSON, all metadata attributes are present before rows
                    Object.keys(data).forEach((key) => {
                        if (Object.keys(parsedMetadata).includes(key)) {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            (metadata as any)[key as MetadataAttributes] =
                                data[key as MetadataAttributes];
                            parsedMetadata[key as MetadataAttributes] = true;
                        }
                    });
                    // Check if all required elements were parsed
                    if (this.checkAttributesParsed(parsedMetadata)) {
                        this.metadataLoaded = true;
                        this.metadata = metadata;
                        resolve(metadata);
                        this.stream?.destroy();
                    }
                })
                .on('footer', (data: DatasetMetadata) => {
                    // If not all required metadata attributes were found before rows, check if they are present after
                    Object.keys(data).forEach((key) => {
                        if (Object.keys(parsedMetadata).includes(key)) {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            (metadata as any)[key as MetadataAttributes] =
                                data[key as MetadataAttributes];
                            parsedMetadata[key as MetadataAttributes] = true;
                        }
                    });
                    // Check if all required elements were parsed
                    if (this.checkAttributesParsed(parsedMetadata)) {
                        this.metadataLoaded = true;
                        this.metadata = metadata;
                        resolve(metadata);
                        this.stream?.destroy();
                    }
                });
        });
    }

    /**
     * Get Dataset-JSON metadata when the file is in NDJSON format.
     * @return An object with file metadata.
     */
    private async getNdjsonMetadata(): Promise<DatasetMetadata> {
        return new Promise((resolve, reject) => {
            this.metadataLoaded = false;
            // All metadata is stored in the first line of the file
            const metadata: DatasetMetadata = {
                datasetJSONCreationDateTime: '',
                datasetJSONVersion: '',
                records: -1,
                name: '',
                label: '',
                columns: [],
                studyOID: '',
                metaDataVersionOID: '',
            };
            const parsedMetadata: ParsedAttributes = {
                datasetJSONCreationDateTime: false,
                datasetJSONVersion: false,
                dbLastModifiedDateTime: false,
                fileOID: false,
                originator: false,
                sourceSystem: false,
                itemGroupOID: false,
                columns: false,
                records: false,
                name: false,
                label: false,
                studyOID: false,
                metaDataVersionOID: false,
                metaDataRef: false,
            };

            // Restart stream
            if (
                this.stream === null ||
                this.currentPosition !== 0 ||
                this.stream?.destroyed
            ) {
                if (this.stream !== null && !this.stream?.destroyed) {
                    this.stream?.destroy();
                }
                if (this.isCompressed) {
                    const rawStream = fs.createReadStream(this.filePath);
                    const gunzip = zlib.createGunzip();
                    this.stream = rawStream.pipe(gunzip);
                } else {
                    this.stream = fs.createReadStream(this.filePath, {
                        encoding: this.encoding,
                    });
                }
            }

            if (this.stream === null) {
                reject(
                    new Error(
                        'Could not create read stream for file ' + this.filePath
                    )
                );
                return;
            }

            this.rlStream = readline.createInterface({
                input: this.stream,
                crlfDelay: Infinity,
            });

            this.rlStream.on('line', (line) => {
                const data = JSON.parse(line);
                // Fill metadata with parsed attributes
                Object.keys(data).forEach((key) => {
                    if (Object.keys(parsedMetadata).includes(key)) {
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        (metadata as any)[key as MetadataAttributes] =
                            data[key as MetadataAttributes];
                        parsedMetadata[key as MetadataAttributes] = true;
                    }
                });
                // Check if all required elements were parsed
                if (this.checkAttributesParsed(parsedMetadata)) {
                    this.metadataLoaded = true;
                    this.metadata = metadata;
                    resolve(metadata);
                } else {
                    const notParsed = Object.keys(parsedMetadata).filter(
                        (key) =>
                            !parsedMetadata[key as MetadataAttributes] &&
                            this.requiredAttributes.includes(key)
                    );
                    reject(
                        new Error(
                            'Could not find required metadata elements: ' +
                                notParsed.join(', ')
                        )
                    );
                }
                if (this.rlStream !== undefined) {
                    this.rlStream.close();
                }
                this.stream?.destroy();
            });
        });
    }

    /**
     * Read observations.
     * @param start - The first row number to read.
     * @param length - The number of records to read.
     * @param type - The type of the returned object.
     * @param filterColumns - The list of columns to return when type is object. If empty, all columns are returned.
     * @param filter - A filter class object used to filter data records when reading the dataset.
     * @return An array of observations.
     */
    async getData(props: {
        start?: number;
        length?: number;
        type?: DataType;
        filterColumns?: string[];
        filter?: Filter;
    }): Promise<(ItemDataArray | ItemDataObject)[]> {
        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        let { filterColumns = [] } = props;

        // Convert filterColumns to lowercase for case-insensitive comparison
        filterColumns = filterColumns.map((item) => item.toLowerCase());

        // Check if metadata is loaded
        if (
            this.metadata.columns.length === 0 ||
            this.metadata.records === -1
        ) {
            return Promise.reject(
                new Error('Metadata is not loaded or there are no columns')
            );
        }
        const { start = 0, length } = props;
        // Check if start and length are valid
        if (
            (typeof length === 'number' && length <= 0) ||
            start < 0 ||
            start > this.metadata.records
        ) {
            return Promise.reject(
                new Error('Invalid start/length parameter values')
            );
        }
        if (this.isNdJson) {
            return this.getNdjsonData({ ...props, filterColumns });
        } else {
            return this.getJsonData({ ...props, filterColumns });
        }
    }

    private async getJsonData(props: {
        start?: number;
        length?: number;
        type?: DataType;
        filterColumns?: string[];
        filter?: Filter;
    }): Promise<(ItemDataArray | ItemDataObject)[]> {
        // Default type to array;
        const { start = 0, length, type = 'array', filter } = props;

        const filterColumns = props.filterColumns as string[];
        const filterColumnIndeces = filterColumns.map((column) =>
            this.metadata.columns.findIndex(
                (item) => item.name.toLowerCase() === column.toLowerCase()
            )
        );

        return new Promise((resolve, reject) => {
            // Validate parameters
            const columnNames: string[] = [];
            if (type === 'object') {
                columnNames.push(
                    ...this.metadata.columns.map((item) => item.name)
                );
            }
            // If possible, continue reading existing stream, otherwise recreate it.
            let currentPosition = this.currentPosition;
            if (
                this.stream === null ||
                this.stream.destroyed ||
                currentPosition > start
            ) {
                if (this.stream !== null && !this.stream.destroyed) {
                    this.stream.destroy();
                }
                this.stream = fs.createReadStream(this.filePath, {
                    encoding: this.encoding,
                });
                currentPosition = 0;
                this.parser = JSONStream.parse(
                    ['rows', true],
                    (data: string, nodePath: string) => {
                        return { path: nodePath, value: data };
                    }
                ) as JSONStreamParser;
                this.stream.pipe(this.parser as unknown as fs.WriteStream);
            }

            if (this.parser === undefined) {
                reject(new Error('Could not create JSON parser'));
                return;
            }

            const currentData: (ItemDataArray | ItemDataObject)[] = [];
            let filteredRecords = 0;
            const isFiltered = filter !== undefined;

            this.parser
                .on('end', () => {
                    resolve(currentData);
                    this.allRowsRead = true;
                })
                .on('data', (data: { path: string; value: ItemDataArray }) => {
                    currentPosition += 1;
                    if (
                        length === undefined ||
                        (currentPosition > start &&
                            (isFiltered
                                ? filteredRecords < length
                                : currentPosition <= start + length))
                    ) {
                        if (!isFiltered || filter.filterRow(data.value)) {
                            if (type === 'array') {
                                if (isFiltered) {
                                    filteredRecords += 1;
                                }
                                if (filterColumnIndeces.length === 0) {
                                    currentData.push(
                                        data.value as ItemDataArray
                                    );
                                } else {
                                    // Keep only indeces specified in filterColumnIndeces
                                    currentData.push(
                                        data.value.filter((_, index) =>
                                            filterColumnIndeces.includes(index)
                                        )
                                    );
                                }
                            } else if (type === 'object') {
                                const obj: ItemDataObject = {};
                                if (filterColumns.length === 0) {
                                    columnNames.forEach((name, index) => {
                                        obj[name] = data.value[index];
                                    });
                                } else {
                                    // Keep only attributes specified in filterColumns
                                    columnNames.forEach((name, index) => {
                                        if (
                                            filterColumns.includes(
                                                name.toLowerCase()
                                            )
                                        ) {
                                            obj[name] = data.value[index];
                                        }
                                    });
                                }
                                if (isFiltered) {
                                    filteredRecords += 1;
                                }
                                currentData.push(obj);
                            }
                        }
                    }

                    if (
                        length !== undefined &&
                        (isFiltered
                            ? filteredRecords === length
                            : currentPosition === start + length) &&
                        this.parser !== undefined
                    ) {
                        const parser = this.parser;
                        // Pause the stream and remove current event listeners
                        parser.pause();
                        parser.removeAllListeners('end');
                        parser.removeAllListeners('data');
                        this.currentPosition = currentPosition;
                        resolve(currentData);
                    }
                });
            // Resume the stream if it was paused
            if ((this.parser as unknown as { paused: boolean }).paused) {
                // Remove previous data
                this.parser.resume();
            }
        });
    }

    private async getNdjsonData(props: {
        start?: number;
        length?: number;
        type?: DataType;
        filterColumns?: string[];
        filter?: Filter;
    }): Promise<(ItemDataArray | ItemDataObject)[]> {
        return new Promise((resolve, reject) => {
            // Default type to array;
            const { start = 0, length, type = 'array', filter } = props;
            const filterColumns = props.filterColumns as string[];
            const filterColumnIndeces = filterColumns.map((column) =>
                this.metadata.columns.findIndex(
                    (item) => item.name.toLowerCase() === column.toLowerCase()
                )
            );

            // If possible, continue reading existing stream, otherwise recreate it.
            let currentPosition = this.currentPosition;
            if (
                this.stream === null ||
                this.stream.destroyed ||
                currentPosition > start
            ) {
                if (this.stream !== null && !this.stream.destroyed) {
                    this.stream.destroy();
                }
                if (this.isCompressed) {
                    const rawStream = fs.createReadStream(this.filePath);
                    const gunzip = zlib.createGunzip();
                    this.stream = rawStream.pipe(gunzip);
                } else {
                    this.stream = fs.createReadStream(this.filePath, {
                        encoding: this.encoding,
                    });
                }
                currentPosition = 0;
                this.rlStream = readline.createInterface({
                    input: this.stream,
                    crlfDelay: Infinity,
                });
            }

            if (this.rlStream === undefined) {
                reject(new Error('Could not create readline stream'));
                return;
            }

            const columnNames: string[] = [];
            if (type === 'object') {
                columnNames.push(
                    ...this.metadata.columns.map((item) => item.name)
                );
            }

            const currentData: (ItemDataArray | ItemDataObject)[] = [];
            // First line contains metadata, so skip it when reading the data
            let isFirstLine = true;
            let filteredRecords = 0;
            const isFiltered = filter !== undefined;

            this.rlStream
                .on('line', (line) => {
                    if (currentPosition === 0 && isFirstLine) {
                        isFirstLine = false;
                        return;
                    }
                    currentPosition += 1;
                    if (
                        (length === undefined ||
                            (currentPosition > start &&
                                (isFiltered
                                    ? filteredRecords < length
                                    : currentPosition <= start + length))) &&
                        line.length > 0
                    ) {
                        const data = JSON.parse(line);
                        if (!isFiltered || filter.filterRow(data)) {
                            if (type === 'array') {
                                if (isFiltered) {
                                    filteredRecords += 1;
                                }
                                if (filterColumnIndeces.length === 0) {
                                    currentData.push(data as ItemDataArray);
                                } else {
                                    // Keep only indeces specified in filterColumnIndeces
                                    currentData.push(
                                        (data as ItemDataArray).filter(
                                            (_, index) =>
                                                filterColumnIndeces.includes(
                                                    index
                                                )
                                        )
                                    );
                                }
                            } else if (type === 'object') {
                                const obj: ItemDataObject = {};
                                if (filterColumns.length === 0) {
                                    columnNames.forEach((name, index) => {
                                        obj[name] = data[index];
                                    });
                                } else {
                                    // Keep only attributes specified in filterColumns
                                    columnNames.forEach((name, index) => {
                                        if (
                                            filterColumns.includes(
                                                name.toLowerCase()
                                            )
                                        ) {
                                            obj[name] = data[index];
                                        }
                                    });
                                }
                                if (isFiltered) {
                                    filteredRecords += 1;
                                }
                                currentData.push(obj);
                            }
                        }
                    }
                    if (
                        length !== undefined &&
                        (isFiltered
                            ? filteredRecords === length
                            : currentPosition === start + length)
                    ) {
                        // When pausing readline, it does not stop immidiately and can emit extra lines,
                        // so pausing approach is not yet implemented
                        if (this.rlStream !== undefined) {
                            this.rlStream.close();
                        }
                        this.stream?.destroy();
                        this.currentPosition = 0;
                        resolve(currentData);
                    }
                })
                .on('error', (err) => {
                    reject(err);
                })
                .on('close', () => {
                    resolve(currentData);
                    this.allRowsRead = true;
                });
        });
    }

    /**
     * Read observations as an iterable.
     * @param start - The first row number to read.
     * @param bufferLength - The number of records to read in a chunk.
     * @param type - The type of the returned object.
     * @param filterColumns - The list of columns to return when type is object. If empty, all columns are returned.
     * @return An iterable object.
     */

    async *readRecords(props?: {
        start?: number;
        bufferLength?: number;
        type?: DataType;
        filterColumns?: string[];
    }): AsyncGenerator<ItemDataArray | ItemDataObject, void, undefined> {
        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        const {
            start = 0,
            bufferLength = 1000,
            type,
            filterColumns,
        } = props || {};
        let currentPosition = start;

        while (true) {
            const data = await this.getData({
                start: currentPosition,
                length: bufferLength,
                type,
                filterColumns,
            });
            yield* data;

            if (this.allRowsRead === true || data.length === 0) {
                break;
            }
            currentPosition = this.currentPosition;
        }
    }

    /**
     * Get unique values observations.
     * @param columns - The list of variables for which to obtain the unique observations.
     * @param limit - The maximum number of values to store. 0 - no limit.
     * @param bufferLength - The number of records to read in a chunk.
     * @param sort - Controls whether to sort the unique values.
     * @return An array of observations.
     */
    async getUniqueValues(props: {
        columns: string[];
        limit?: number;
        bufferLength?: number;
        sort?: boolean;
    }): Promise<UniqueValues> {
        const { limit = 100, bufferLength = 1000, sort = true } = props;
        let { columns } = props;
        const result: UniqueValues = {};

        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        const notFoundColumns: string[] = [];
        // Use the case of the columns as specified in the metadata
        columns = columns.map((item) => {
            const column = this.metadata.columns.find(
                (column) => column.name.toLowerCase() === item.toLowerCase()
            );
            if (column === undefined) {
                notFoundColumns.push(item);
                return '';
            } else {
                return column.name as string;
            }
        });

        if (notFoundColumns.length > 0) {
            return Promise.reject(
                new Error(`Columns ${notFoundColumns.join(', ')} not found`)
            );
        }

        // Store number of unique values found
        const uniqueCount: { [name: string]: number } = {};
        columns.forEach((column) => {
            uniqueCount[column] = 0;
        });

        let isFinished = false;

        for await (const row of this.readRecords({
            bufferLength,
            type: 'object',
            filterColumns: columns,
        }) as AsyncGenerator<ItemDataObject>) {
            columns.forEach((column) => {
                if (result[column] === undefined) {
                    result[column] = [];
                }
                if (
                    uniqueCount[column] < limit &&
                    row[column] !== null &&
                    !result[column].includes(row[column])
                ) {
                    result[column].push(row[column]);
                    uniqueCount[column] += 1;
                }
            });

            // Check if all unique values are found
            isFinished = Object.keys(uniqueCount).every(
                (key) => uniqueCount[key] >= limit
            );

            if (isFinished) {
                break;
            }
        }

        // Sort result
        if (sort) {
            Object.keys(result).forEach((key) => {
                result[key].sort();
            });
        }

        return result;
    }

    /**
     * Helper method to safely write data to stream with backpressure handling
     * @param data - String data to write
     */
    private async writeWithBackpressure(data: string): Promise<void> {
        // Create new Promise for this write operation
        const writeOperation = this.writeQueueDrain.then(() => {
            return new Promise<void>((resolve) => {
                if (!this.writeStream?.write(data)) {
                    this.writeStream?.once('drain', () => resolve());
                } else {
                    resolve();
                }
            });
        });

        // Update queue with current operation
        this.writeQueueDrain = writeOperation;

        // Wait for this write to complete
        await writeOperation;
    }

    /**
     * Write data to the file
     * @param props.metadata - Dataset metadata
     * @param props.data - Data to write
     * @param props.action - Write action: create, write, or finalize
     * @param props.options - Write options (prettify, highWaterMark)
     */
    async write(props: {
        metadata?: DatasetMetadata;
        data?: ItemDataArray[];
        action: 'create' | 'write' | 'finalize';
        options?: {
            prettify?: boolean;
            highWaterMark?: number;
            indentSize?: number;
            compressionLevel?: number;
        };
    }): Promise<void> {
        const { metadata, data, action, options = {} } = props;
        const {
            highWaterMark = 16384, // 16KB default
            indentSize = 2,
            compressionLevel = 9,
        } = options;

        let { prettify = false } = options;

        // In case of compressed file, prettify must be false
        if (this.isCompressed && prettify) {
            prettify = false;
        }

        if (action === 'create') {
            if (!metadata) {
                throw new Error('Metadata is required for create action');
            }

            this.writeMode = this.isNdJson ? 'ndjson' : 'json';
            this.isFirstWrite = true;

            if (this.isCompressed) {
                // Create gzip stream
                const outputStream = fs.createWriteStream(this.filePath, {
                    encoding: this.encoding,
                    highWaterMark,
                });
                const gzip = zlib.createGzip({ level: compressionLevel });
                gzip.pipe(outputStream);
                this.writeStream = gzip;
            } else {
                this.writeStream = fs.createWriteStream(this.filePath, {
                    encoding: this.encoding,
                    highWaterMark,
                });
            }

            if (this.writeMode === 'json') {
                // Remove rows from metadata to avoid empty array
                let initialStr = prettify
                    ? JSON.stringify(metadata, null, indentSize)
                    : JSON.stringify(metadata);
                // Remove closing brace and add rows array opening
                initialStr = initialStr.slice(0, -1);
                // In case of prettify, remove last new line
                if (prettify && initialStr.endsWith('\n')) {
                    initialStr = initialStr.slice(0, -1);
                }
                // Add rows array opening
                initialStr =
                    initialStr +
                    (prettify
                        ? ',\n' + ' '.repeat(indentSize) + '"rows": ['
                        : ',"rows":[');
                await this.writeWithBackpressure(initialStr);
            } else {
                await this.writeWithBackpressure(
                    JSON.stringify(metadata) + '\n'
                );
            }

            if (data) {
                await this.write({ data, action: 'write', options });
            }
        } else if (action === 'write') {
            if (!this.writeStream) {
                throw new Error('No active write stream. Call create first.');
            }
            if (!data || !data.length) {
                return;
            }

            if (this.writeMode === 'json') {
                for (let i = 0; i < data.length; i++) {
                    const prefix = this.isFirstWrite && i === 0 ? '' : ',';
                    const rowStr = prettify
                        ? prefix +
                          '\n' +
                          ' '.repeat(indentSize * 2) +
                          JSON.stringify(data[i])
                        : prefix + JSON.stringify(data[i]);
                    await this.writeWithBackpressure(rowStr);
                }
            } else {
                for (const row of data) {
                    await this.writeWithBackpressure(
                        JSON.stringify(row) + '\n'
                    );
                }
            }
            this.isFirstWrite = false;
        } else if (action === 'finalize') {
            if (!this.writeStream) {
                throw new Error('No active write stream. Call create first.');
            }

            if (data) {
                await this.write({ data, action: 'write', options });
            }

            if (this.writeMode === 'json') {
                await this.writeWithBackpressure(
                    prettify ? '\n' + ' '.repeat(indentSize) + ']\n}' : ']}'
                );
            }

            // Wait for all writes to complete and close stream
            await new Promise<void>((resolve, reject) => {
                this.writeStream?.end(() => {
                    this.writeStream = undefined;
                    this.writeMode = undefined;
                    this.isFirstWrite = true;
                    resolve();
                });
                this.writeStream?.on('error', reject);
            });
        }
    }

    /**
     * Write data to file in one operation
     * @param props.metadata - Dataset metadata
     * @param props.data - Data to write
     * @param props.options - Write options (prettify, highWaterMark)
     */
    async writeData(props: {
        metadata: DatasetMetadata;
        data?: ItemDataArray[];
        options?: {
            prettify?: boolean;
            highWaterMark?: number;
            indentSize?: number;
        };
    }): Promise<void> {
        const { metadata, data, options } = props;

        // Create file and write metadata
        await this.write({
            metadata,
            action: 'create',
            options,
        });

        // Write data if provided
        if (data?.length) {
            await this.write({
                data,
                action: 'write',
                options,
            });
        }

        // Finalize the file
        await this.write({
            action: 'finalize',
            options,
        });
    }
}

export default DatasetJson;
