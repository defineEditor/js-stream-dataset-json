// Variable Types
export type ItemType =
    | 'string'
    | 'integer'
    | 'float'
    | 'double'
    | 'decimal'
    | 'boolean'
    | 'date'
    | 'time'
    | 'datetime'
    | 'URI'
    ;

// Target Variable Types
export type ItemTargetType =
    | 'integer'
    | 'decimal';

// Type of returned Object from getData
export type ItemDataObject = { [name: string]: string | number | boolean | null };

// The first item in the data array needs to be a number (itemGroupDataSeq)
export type ItemDataArray = Array<string | number | boolean | null >;

// Source System interface
export interface SourceSystem {
    // Name of the Source System
    name: string;
    // Version of the Source System
    version: string;
}


// Definition for Variable in the Dataset
export interface ItemDescription {
    // Unique identifier for Variable. Must correspond to ItemDef/@OID in Define-XML.
    itemOID: string;
    // Name for Variable
    name: string;
    // Label for Variable
    label: string;
    // Data type for Variable
    dataType: ItemType;
    // Indicates the data type into which the receiving system must transform the associated Dataset-JSON variable.
    targetDataType?: ItemType;
    // Length for Variable
    length?: number;
    // Display format supports data visualization of numeric float and date values.
    displayFormat?: string;
    // Indicates that this item is a key variable in the dataset structure. It also provides an ordering for the keys.
    keySequence?: number;
}


// Definition for Dataset-JSON
export interface Dataset {
    // Time of creation of the file containing the document.
    datasetJSONCreationDateTime: string;
    // Version of Dataset-JSON standard.
    datasetJSONVersion: string;
    // The total number of records in a dataset
    records: number;
    // The human readable name of the dataset
    name: string;
    // A short description of the dataset
    label: string;
    // Variable metadata
    columns: Array<ItemDescription>;
    // Data
    rows: Array<ItemDataArray>;
    // The date/time source database was last modified.
    dbLastModifiedDateTime? : string;
    // A unique identifier for this file.
    fileOID?: string;
    // The organization that generated the Dataset-JSON file.
    originator?: string;
    // The computer system or database management system that is the source of the information in this file.
    sourceSystem?: SourceSystem;
    // Unique identifier for Study. See ODM definition for study OID (ODM/Study/@OID).
    studyOID?: string;
    // Metadata for the data contained in the file.
    metaDataVersionOID?: string;
    // URL for a metadata file the describing the data.
    metaDataRef?: string;
    // Foreign key to ItemGroupDef.OID in Define / MDR
    itemGroupOID?: string;
}

// Metadata for the dataset
export type DatasetMetadata = Omit<Dataset, 'rows'>;
// Metadata Attributes
export type MetadataAttributes = keyof DatasetMetadata;
// Interface for checking which attributes are parsed
export type ParsedAttributes = {
    [name in MetadataAttributes]: boolean;
}
// Type of the object returned
export type DataType = 'array' | 'object';
export interface UniqueValues {
    [name: string]: {
        values: (string | number | boolean | null)[]
        counts: {[name: string]: number}

    };
}

// Add this interface near the top of the file with other interfaces
export interface JSONStreamParser {
    on(event: 'data' | 'end', listener: (data: {path: string, value: ItemDataArray}) => void): this;
    pause(): void;
    resume(): void;
    removeAllListeners(event?: string): this;
    paused: boolean;
}