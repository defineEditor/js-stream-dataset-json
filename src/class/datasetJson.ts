import fs from "fs";
import fsPromises from "fs/promises";
import {
    ItemDataArray,
    ItemDataObject,
    DatasetMetadata,
    DataType,
    UniqueValues,
    MetadataAttributes,
    ParsedAttributes,
} from "interfaces/datasetJson.d";
import JSONStream from "JSONStream";

// Main class for dataset JSON;
class DatasetJson {
    // Path to the file;
    filePath: string;
    // Statistics about the file;
    stats: fs.Stats;
    // Item Group metadata;
    metadata: DatasetMetadata;

    // Current position in the file;
    currentPosition: number;
    // Flag to indicate if all rows are read;
    allRowsRead: boolean;
    // Metadata loaded
    private metadataLoaded: boolean;
    // Stream
    private stream: fs.ReadStream;
    // Parser
    private parser?: fs.ReadStream;
    // NDJSON
    private isNdJson: boolean;

    /*k*
     * Read observations.
     * @constructor
     * @param filePath - Path to the file.
     */
    constructor(filePath: string, isNdJson: boolean = false) {
        this.filePath = filePath;
        this.currentPosition = 0;
        this.isNdJson = isNdJson;
        this.allRowsRead = false;
        this.metadataLoaded = false;

        this.metadata = {
            datasetJSONCreationDateTime: "",
            datasetJSONVersion: "",
            records: -1,
            name: "",
            label: "",
            columns: [],
        }

        // Check if file exists;
        if (!fs.existsSync(this.filePath)) {
            throw new Error(`Could not read file ${this.filePath}`);
        }

        this.stats = fs.statSync(this.filePath);

        this.stream = fs.createReadStream(this.filePath, {
            encoding: "utf8",
        });

    }

    /**
     * Check if the file was modified
     * @return True if file has changed, otherwise false.
     */
    async #fileChanged(): Promise<boolean> {
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
    #checkAttributesParsed = (
        item: { [name: string]: boolean }
    ): boolean => {
        const requiredAttributes = ["datasetJSONCreationDateTime", "datasetJSONVersion", "records", "name", "label", "columns"];

        return requiredAttributes.every((key) => item[key] === true);
    };

    /**
     * Get Dataset-JSON metadata
     * @return An object with file metadata.
     */
    async getMetadata(): Promise<DatasetMetadata> {

        if (this.isNdJson) {
            return Promise.reject(new Error("NDJSON is not supported yet"));
        }
        // If the file did not change, use the metadata obtained during initialization;
        if (!(await this.#fileChanged()) && this.metadataLoaded === true) {
            return this.metadata;
        } else {
            return new Promise((resolve, reject) => {
                this.metadataLoaded = false;
                // Metadata for ItemGroup
                const metadata: DatasetMetadata = {
                    datasetJSONCreationDateTime: "",
                    datasetJSONVersion: "",
                    records: -1,
                    name: "",
                    label: "",
                    columns: [],
                    studyOID: "",
                    metaDataVersionOID: "",
                };
                const parsedMetadata: ParsedAttributes = {
                    datasetJSONCreationDateTime: false,
                    datasetJSONVersion: false,
                    dbLastModifiedDateTime: false,
                    fileOID: false,
                    originator: false,
                    sourceSystem: false,
                    itemGroupOID: false,
                    isReferenceData: false,
                    columns: false,
                    records: false,
                    name: false,
                    label: false,
                    studyOID: false,
                    metaDataVersionOID: false,
                    metaDataRef: false,
                };

                // Restart stream
                if (this.currentPosition !== 0 || this.stream?.destroyed) {
                    if (!this.stream?.destroyed) {
                        this.stream?.destroy();
                    }
                    this.stream = fs.createReadStream(this.filePath, {
                        encoding: "utf8",
                    });
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
                    .on("end", () => {
                        // Check if all required attributes are parsed after the file is fully loaded;
                        if (
                            !this.#checkAttributesParsed(
                                parsedMetadata
                            )
                        ) {
                            reject(
                                new Error(
                                    "Could not find required metadata elements"
                                )
                            );
                        }
                        this.metadataLoaded = true;
                        this.metadata = metadata;
                        resolve(metadata);
                    })
                    .on("header", (data: DatasetMetadata) => {
                        // In correctly formed Dataset-JSON, all metadata attributes are present before rows
                        Object.keys(data).forEach(key => {
                            if (Object.keys(parsedMetadata).includes(key)) {
                                (metadata as any)[key as MetadataAttributes] = data[key as MetadataAttributes];
                                parsedMetadata[key as MetadataAttributes] = true;
                            }
                        });
                        // Check if all required elements were parsed
                        if (this.#checkAttributesParsed(parsedMetadata)) {
                            this.metadataLoaded = true;
                            this.metadata = metadata;
                            resolve(metadata);
                            this.stream.destroy();
                        }
                    })
                    .on("footer", (data: DatasetMetadata) => {
                        // If not all required metadata attributes were found before rows, check if they are present after
                        Object.keys(data).forEach(key => {
                            if (Object.keys(parsedMetadata).includes(key)) {
                                (metadata as any)[key as MetadataAttributes] = data[key as MetadataAttributes];
                                parsedMetadata[key as MetadataAttributes] = true;
                            }
                        });
                        // Check if all required elements were parsed
                        if (this.#checkAttributesParsed(parsedMetadata)) {
                            this.metadataLoaded = true;
                            this.metadata = metadata;
                            resolve(metadata);
                            this.stream.destroy();
                        }
                    });
            });
        }
    }

    /**
     * Read observations.
     * @param start - The first row number to read.
     * @param length - The number of records to read.
     * @param type - The type of the returned object.
     * @param filterColumns - The list of columns to return when type is object. If empty, all columns are returned.
     * @return An array of observations.
     */
    async getData(props: {start: number, length?: number, type?: DataType, filterColumns?: string[]}): Promise<(ItemDataArray|ItemDataObject)[]> {
        if (this.isNdJson) {
            return Promise.reject(new Error("NDJSON is not supported yet"));
        }

        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        // Default type to array;
        const { start, length, type = "array" } = props;

        let { filterColumns = [] } = props;

        // Convert filterColumns to lowercase for case-insensitive comparison
        filterColumns = filterColumns.map((item) => item.toLowerCase());

        // Check if metadata is loaded
        if (this.metadata.columns.length === 0 || this.metadata.records === -1) {
            return Promise.reject(new Error("Metadata is not loaded or there are no columns"));
        }

        return new Promise((resolve, reject) => {
            // Validate parameters
            const columnNames: string[] = [];
            if (type === "object") {
                columnNames.push(...this.metadata.columns.map((item) => item.name));
            }
            if ((typeof length === 'number' && length <= 0) || start < 0 || start > this.metadata.records) {
                reject(new Error("Invalid parameter values"));
            }
            // If possible, continue reading existing stream, otherwise recreate it.
            let currentPosition = this.currentPosition;
            if (this.stream.destroyed || currentPosition > start) {
                if (!this.stream.destroyed) {
                    this.stream.destroy();
                }
                this.stream = fs.createReadStream(this.filePath, {
                    encoding: "utf8",
                });
                currentPosition = 0;
                this.parser =
                        JSONStream.parse(
                            [
                                'rows',
                                true,
                            ],
                            (data: string, nodePath: string) => {
                                return { path: nodePath, value: data };
                            }
                        ) as fs.ReadStream;
                this.stream
                    .pipe(this.parser as unknown as fs.WriteStream)
            }

            if (this.parser === undefined) {
                reject(new Error("Could not create JSON parser"));
                return;
            }

            const currentData: (ItemDataArray|ItemDataObject)[] = [];

            this.parser
                .on("end", () => {
                    resolve(currentData);
                    this.allRowsRead = true;
                })
                .on("data", (data: { path: string; value: ItemDataArray }) => {
                    currentPosition += 1;
                    if (
                        length === undefined ||
                        (currentPosition > start &&
                            currentPosition <= start + length)
                    ) {
                        if (type === "array") {
                            currentData.push(data.value as ItemDataArray);
                        } else if (type === "object") {
                            const obj: ItemDataObject = {};
                            if (filterColumns.length === 0) {
                                columnNames.forEach((name, index) => {
                                    obj[name] = data.value[index];
                                });
                            } else {
                                // Keep only attributes specified in filterColumns
                                columnNames.forEach((name, index) => {
                                    if (filterColumns.includes(name.toLowerCase())) {
                                        obj[name] = data.value[index];
                                    }
                                });
                            }
                            currentData.push(obj);
                        }
                    }

                    if (length !== undefined && currentPosition === start + length) {
                        const parser = this.parser as fs.ReadStream;
                        // Pause the stream and remove current event listeners
                        parser.pause();
                        parser.removeAllListeners("end");
                        parser.removeAllListeners("data");
                        this.currentPosition = currentPosition;
                        resolve(currentData);
                    }
                });
            // Resume the stream if it was paused
            if ((this.parser as unknown as {paused: boolean}).paused) {
                // Remove previous data
                this.parser.resume();
            }

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

    async *readRecords(props?: {start?: number, bufferLength?: number, type?: DataType, filterColumns?: string[]}): AsyncGenerator<ItemDataArray|ItemDataObject, void, undefined> {

        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        const { start = 0, bufferLength = 1000, type, filterColumns } = props || {};
        let currentPosition = start;

        while (true) {
            const data = await this.getData({ start: currentPosition, length: bufferLength, type, filterColumns });
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
        columns: string[],
        limit?: number,
        bufferLength?: number
        sort?: boolean
    }): Promise<UniqueValues> {

        const { limit = 100, bufferLength = 1000, sort = true} = props;
        let { columns } = props;
        let result: UniqueValues = {};


        // Check if metadata is loaded
        if (this.metadataLoaded === false) {
            await this.getMetadata();
        }

        const notFoundColumns: string[] = [];
        // Use the case of the columns as specified in the metadata
        columns = columns.map((item) => {
            const column = this.metadata.columns.find((column) => column.name.toLowerCase() === item.toLowerCase());
            if (column === undefined) {
                notFoundColumns.push(item);
                return "";
            } else {
                return column.name as string;
            }
        });

        if (notFoundColumns.length > 0) {
            return Promise.reject(new Error(`Columns ${notFoundColumns.join(", ")} not found`));
        }

        // Store number of unique values found
        const uniqueCount: {[name: string]: number} = {};
        columns.forEach((column) => {
            uniqueCount[column] = 0;
        });

        let isFinished = false;

        for await(const row of this.readRecords({bufferLength, type: "object", filterColumns: columns}) as AsyncGenerator<ItemDataObject>) {
            columns.forEach((column) => {
                if (result[column] === undefined) {
                    result[column] = [];
                }
                if (uniqueCount[column] < limit && row[column] !== null && !result[column].includes(row[column])) {
                    result[column].push(row[column]);
                    uniqueCount[column] += 1;
                }
            });

            // Check if all unique values are found
            isFinished =  Object.keys(uniqueCount).every((key) => uniqueCount[key] >= limit);

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
}

export default DatasetJson;
