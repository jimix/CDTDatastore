## Using Core Data

> ***Warning***: This document assumes you are familiar with
> [Core Data].

From the [Apple documents][core data]:
>  The Core Data framework provides generalized and automated
>  solutions to common tasks associated with object life-cycle and
>  object graph management, including persistence.

It is this *"persistence"*, which is provided by the
[Persistent Store], that we wish to add `CDTDatastore` as a backing
store. The [Incremental Store] provides the hooks necessary to do
this.

Thankfully, the user does not need to know these details to exploit
`CDTDatastore` from an application that uses [Core Data].

### Example Application

There is an example application based on
[Apple's iPhoneCoreDataRecipes][recipe] and can be found in this
[git tree][gitrecipe].

### Getting started

A `CDTIncrementalStore` object is used to "link" everything
together. In order for [Core Data] to recognize
`CDTIncrementalStore` as a store, it must be initialized. This happens
automatically in OSX, but must be called manually in iOS, usually in
the `UIApplicationDelegate`, example:

```objc
- (BOOL)application:(UIApplication *)application didFinishLaunchingWithOptions:(NSDictionary *)launchOptions {

    [CDTIncrementalStore initialize];
	...

```

> ***Note***: it is safe to call `+initialize` multiple times.

Then you may use [Core Data] as usual, however, now when you set up
your persistent store you request the `CDTIncrementalStore` in the
following manner, using `+type` static method to identify the correct
store type:

```objc
NSURL *storeURL = [NSURL URLWithString:databaseURI];
NSString *myType = [CDTIncrementalStore type];
NSPersistentStoreCoordinator *psc = ...
[psc addPersistentStoreWithType:myType
                  configuration:nil
                            URL:storeURL
                        options:nil
                          error:&error])];
```

The last component of `databaseURI` should be the name of your
database. If `databaseURI` has a host component, then that URL will be
used to specify the remote database where push, pull and sync
operations will target.

> ***Note***: the remote database details can be defined later in your
> code.

At this point you can use [Core Data] normally and your changes will
be saved in the local `CDTDatastore` image.

## Accessing Your Data Store Object

At any time during your application you may decide to access the
remote `CDTDatastore`.  In order to do this from a [Core Data]
application, you need to access the `CDTIncrementalStore` object
instance you require.  Since [Core Data] can have multiple active
stores *and* even several `CDTIncrementalStore` objects, we can use
`+storesFromCoordinator:coordinator` to obtain these objects. If there
is only one `CDTIncrementalStore` object then this is simply:

```objc
// Get our stores
NSArray *stores = [CDTIncrementalStore storesFromCoordinator:psc];
// We know there is only one
CDTIncrementalStore *myIS = [stores firstObject];
```

### Replication

The act of [replication] can be performed by the using the
CloudantSync replication interfaces described in [Replication].

In order to perform a _push_ or a _pull_ you need to obtain a
`CDTISReplicator` that performs the operation. These are provided by
`-replicatorThatPushesToURL:withError:` and
`-replicatorThatPullsFromURL:withError:`.  This object not only
contains a `CDTReplicator` object called `replicator` that is capable
of performing the [replication], it also contains additional methods
to help with merge conflict particularly after a _pull_, these will be
described in detail below.

Once you have a `CDTReplicator` you can use it as instructed in
[Replication], including setting a delegate.

A simple example:

```objc
NSError *err = nil;
CDTIncrementalStore *myIS = <# See: Accessing Your Data Store Object #>
CDTISReplicator *pullerManager = [myIS replicatorThatPullsFromURL:self.remoteURL withError:&err];
CDTReplicator *puller = pullerManager.replicator;

if (![puller startWithError:&err]) {
    [self reportIssue:@"Pull Failed with error: %@", err];
    self.syncButton.enabled = self.serverVerified;
} else {
    while (puller.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
    }
}
```

Synchronization can be taken care of by some combination of _push_ and
_pull_, see the CloudantSync [replication] documents for more
information.

### Conflict Management
Since _pushing_ and _pulling_ happen at the CloudantSync "Datastore"
level, a mechanism to present possible conflicts in a way that is
familiar to the [Core Data] developer is necessary.

Normally, such conflicts are discovered upon saving an
`NSManagedObjectContext` as described in Apple's [Change Management]
documentation.  Ultimately, the programmer is presented with an
`NSArray` of `NSMergeConflict` objects.  These [merge conflict]
objects describe how the managed object has changed between the cached
state at the persistent store coordinator and the external store in
the backing store.

In order to allow these conflicts to be resolved immediately after a
_pull_ has completed, the `CDTISReplicator` object has introduced a
method called `-processConflictsWithContext:error:` which returns the
same `NSArray` of `NSMergeConflict` objects.

This array can be used by the standard [merge policies] provided by
`NSMergePolicyClass` as any [Core Data] application would use.  An
example of using the policy that selects the ones that were _pull_ed in,
over the objects that were active before the _pull_ is as follows:

```objc
// After a successful pull
NSManagedObjectContext *moc =  <# Any active context #>;
NSArray *mergeConflicts = [puller processConflictsWithContext:moc error:nil];
NSMergePolicy *mp = [[NSMergePolicy alloc] initWithMergeType:NSMergeByPropertyStoreTrumpMergePolicyType];
if (![mp resolveConflicts:conflicts error:nil]) {
    // conflict resolution failure
}
```

### Best Practices

1. Save all `NSManagedObjectContext` objects before any replication activity
1. Do not _push_ a Datastore that has conflicts in it.
1. Resolve conflicts immediately after a _pull_.


## The Document Store
Translating [Core Data] objects to a document store requires some
special treatment. Not only do we desire the resulting remote store to
be as portable as possible to other platforms and architectures, we
need to preserve the Meta Data that [Core Data] requires, and also
make sure that we do not compromise the integrity of each data
"entity".

There are two types of documents in the database:
1. Meta Data: there is only one instance of this document and contains
   information about the rest of the documents in the database
1. Entity: Every [Core Data] Entity is represented by a unique entity
   document.

### Meta Data
Each initialized database will contain at least one document that
describes the object model and the [Core Data] Meta Data. The document
ID is `CDTISMetaData` and it is described in the following JSON [Schema], draft v4:

```javascript
// validated by https://json-schema-validator.herokuapp.com/
{
    "id": "CDTISMetaData#",
    "title": "CDTISMetaData Schema",
    "definitions": {
        // We restrict this to C symbol character set
        "symbolicName": {
            "id": "#symbolicName",
            "type": "string",
            "pattern": "^[a-zA-Z][a-zA-Z0-9]*$"
        },

        // Used to contain 512 bit hash
        "hex64": {
            "id": "#hex64",
            "type": "string",
            "pattern": "^[a-fA-F0-9]{64}$"
        },

        // Types we support
        "typeName": {
            "id": "#typeName",
            "enum": [
                // string in UTF-8 encoding
                "utf8",
                // Number 0 for false and anything else for true
                "bool",
                // number of seconds since midnight 1970-01-01 GMT
                "date1970",
                // integer values
                "int16", "int32", "int64",
                // IEEE-754 floating point types
                "double", "float",
                // Large precision decimal values, created by
                // Apple.  Should be avoided for portability,
                "decimal",
                // Transformable Data, the application must provide
                // Class that can transform the stored data into an
                // object usable by the program.
                "xform",
                // An opaque binary WoS
                "binary",
                // Apple Core Data ID URI, this will only be
                // references by Core Data, and if missing will be
                // generated before used.
                "id",
                // Relation (pointer) to single object
                "relation-to-one",
                // Relation to several objects
                "relation-to-many"
            ]
        },

        // this describes the property
        "property": {
            "id": "#property",
            "title": "Property Schema",
            "type": "object",
            "required": [ "versionHash", "name" ],
            "properties": {
                // The hash for this property as generated by Core Data
                "versionHash": { "$ref": "#/definitions/hex64" },
                "typeName": { "$ref": "#/definitions/typeName" },
                // The name of the "class" that can transform the data
                // into a usable object by the platform. Where
                // possible, the mime-type is included to assist in
                // the transformation.
                "xform": { "$ref": "#/definitions/symbolicName" },
                // Destination Entity name for the relation
                "destination": { "$ref": "#/definitions/symbolicName" }
            }
        },

        // Entities are a collection of properties
        "entity": {
            "id": "#entity",
            "title": "Entity Schema",
            "type": "object",
            "required": [ "versionHash", "properties" ],
            "properties": {
                "versionHash": { "$ref": "#/definitions/hex64" },
                "properties": {
                    // A property can have any name, we should
                    // restrict to C symbols but for now we do not.
                    "patternProperties": {
                        "^[^ ]+$": { "$ref": "#/definitions/property" }
                    }
                }
            }
        },

        // Object models describe the stored objects in terms of
        // entities and properties
        "objectModel": {
            "title": "Object Model Schema",
            "type": "object",
            "patternProperties": {
                "^[^ ]+$": { "$ref": "#/definitions/entity" }
            }
        },

        // This is the MetaData as used by Core Data and should be
        // treated as opaque
        "APPLCoreDataMetaData": {
            "title": "Core Data Meta Data Object",
            "type": "object"
        },

        "type": "object",
        "required": [ "metaData", "objectModel" ],
        "properties": {
            "metaData": { "$ref": "#/definitions/APPLCoreDataMetaData" },
            "objectModel": { "$ref": "#/definitions/objectModel" },
            "run": { "type": "string" },
        }
    }
}
```

### Entity Data
All other documents should follow the following schema:

```javascript
// validated by https://json-schema-validator.herokuapp.com/
{
    "title": "CDTISObject",
    "definitions": {
        "symbolicName": {
            "id": "#symbolicName",
            "type": "string",
            "pattern": "^[^ ]+$"
        },

        // The actual storage of a scalar value. In JSON it always
        // comes down to either a number or a string
        "attribute": {
            "id": "#attribute",
            "oneOf": [
                {"type": "string"},
                {"type": "number"}
            ]
        },

        // The string is a Document ID in the database
        "relationToOne": {
            "id": "#relationToOne",
            "type": "string"
        },

        // This is array of Document IDs in the database
        "relationToMany": {
            "id": "#relationToMany",
            "type": "array",
            "items": {"$ref": "#/definitions/relationToOne" }
        },

        // A property is either an attribute or a relation
        "property": {
            "id": "#property",
            "oneOf": [
                { "$ref": "#/definitions/attribute" },
                { "$ref": "#/definitions/relationToOne" },
                { "$ref": "#/definitions/relationToMany" }
            ]
        },

        // The properties that are prefixed with "CDTISMeta_" are
        // objects that contain additional information to support the
        // accurate storage of the data.

        // Binary and Transformable objects have a MIME type
        "metaDataForBinary": {
            "id": "#metaDataForBinary",
            "type": "object",
            "required": [ "mime-type" ],
            "properties": {
                "mime-type": { "type": "string" }
            }
        },

        // Floating Non-Finite values like NaN and +/-Infinity
        // cannot be stored as JSON, so we have added an extra
        // property to capture this.
        "metaDataForFloatNonFinite": {
            "id": "#metaDataForFloatNonFinite",
            "enum": [ "infinity", "-infinity", "nan" ]
        },

        // IEEE single precision floating point values are
        // additionally represented as an integer that captures the
        // 32-bit image.  In C this is can be expressed as:
        //   uint32_t img = *((uint32_t *)&double_num);
        "metaDataForFloatSingle": {
            "id": "#metaDataForFloatSingle",
            "type": "object",
            "required": [ "ieee754_single" ],
            "properties": {
                "nonFinite": { "$ref": "#/definitions/metaDataForFloatNonFinite" },
                "ieee754_single": {"type": "integer" }
            }
        },

        // IEEE double precision floating point values are
        // additionally represented as an integer that captures the
        // 64-bit image.  In C this is can be expressed as:
        //   uint64_t img = *((uint64_t *)&double_num);
        "metaDataForFloatDouble": {
            "id": "#metaDataForFloatDouble",
            "type": "object",
            "required": [ "ieee754_double" ],
            "properties": {
                "nonFinite": { "$ref": "#/definitions/metaDataForFloatNonFinite" },
                "ieee754_double": {"type": "integer" }
            }
        },

        // Apple has a type called NSDecimal, we discourage its use
        // since it is not portable, but we allow it in case the
        // application does not care.
        // The image is a base64 encoding of the structure binary
        "metaDataForNSDecimal": {
            "id": "#metaDataForNSDecimal",
            "type": "object",
            "required": [ "nsdecimal" ],
            "properties": {
                "nonFinite": { "$ref": "#/definitions/metaDataForFloatNonFinite" },
                "nsdecimal": {"type": "string" }
            }
        },

        "metaData": {
            "id": "#metaData",
            "oneOf": [
                { "$ref": "#/definitions/metaDataForBinary" },
                { "$ref": "#/definitions/metaDataForFloatSingle" },
                { "$ref": "#/definitions/metaDataForFloatDouble" },
                { "$ref": "#/definitions/metaDataForNSDecimal" }
            ]
        }
    },

    "type": "object",
    "required": [
        "CDTISEntityName"
    ],

    "properties" : {
        // The entity that this data property belongs to
        "CDTISEntityName": { "$ref": "#/definitions/symbolicName" },

        // URI Generated by Core Data to track this object.  Should
        // never be removed, but this document is not created by Core
        // Data, it can be missing and will be generated once a core
        // data application sees it... I hope
        "CDTISIdentifier": { "type": "string" },

        // I'm only guessing I need this.. probably don't
        "CDTISObjectVersion": { "type": "string" },
    },
    "patternProperties": {
        // Regular properties need to be excluded, is there a better way?
        "^!(CDTIS)[^ ]+$": { "$ref": "#/definitions/property" },
        "^CDTISMeta_[^ ]+$": { "$ref": "#/definitions/metaData" }
    }
}

```

## Portability Issues
To do this we try to
use more generic descriptions for the "meta" information.  Here we
describe some of the more difficult [Core Data] types.

### Time
The number of seconds from January 1, 1970 at 12:00 a.m. GMT.

### Binary Data
Binary data, where the programmer gives no indication of type is
described as "base64" and should be considered of mime-type
"application/octet-stream".

### Transformable Data
[Core Data] applications can provide a class that can transform an
object into some serialized form.  The name of this "Transformer
Class" and the mime-type is stored along with the base64 encoding of
the result.  The mime-type by default is "application/octet-stream".

To increase portability, it is recommended, where possible, that the
programmer add an additional class method called `+MIMEType` that
returns the mime-type of encoded result.  Example method for a class
that transforms to a PNG image.

```objc
+ (NSString *)MIMEType {
    return @"image/png";
}
```

### Arbitrary Decimal Numbers
The `NSDecimalNumber` is described by Apple as:
>  An instance can represent any number that can be expressed as
>  mantissa x 10^exponent where mantissa is a decimal integer up to 38
>  digits long, and exponent is an integer from –128 through 127.

This number is represented in the data-store as a string representation.
> ***Note***: unfortunately this means that you cannot really use this
> value for any predicate based fetches since proper comparison is
> currently impossible.

### Special Floating Point Values
The values `+/-infinity` and `NaN` cannot be expressed in a JSON based
store so they are tokenized accordingly.  In an effort to make
`+/-infinity` evaluated in your predicates, we give them a value of
`+/-MAX_FLT`. See the discussion on Doubles below.

### Double values
Depending on your JSON library, the string encoding and decoding of
doubles can lose some detail.

> ***Note***: This loss of detail may effect how your predicate
> evaluations work.

Regardless of this loss, it is important that the precise original
double value is restored for the [Core Data] objects.  In order to
deal with this inevitable corruption, we also store the IEEE 754
64-bit image as an integer number.

> ***Note***: Since we store the image as a number, there are ***no
> endian issues***.

<!-- refs -->

[core data]: https://developer.apple.com/library/mac/documentation/Cocoa/Conceptual/CoreData/cdProgrammingGuide.html "Introduction to Core Data Programming Guide"
[persistent store]: https://developer.apple.com/library/mac/documentation/Cocoa/Conceptual/CoreData/Articles/cdPersistentStores.html "Persistent Store Features"
[incremental store]: https://developer.apple.com/library/mac/documentation/DataManagement/Conceptual/IncrementalStorePG/Introduction/Introduction.html "About Incremental Stores"
[replication]: replication.md "Replicating Data Between Many Devices"
[conflicts]: conflicts.md "Handling conflicts"
[code blocks]: https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/ProgrammingWithObjectiveC/WorkingwithBlocks/ "Working with Blocks"
[recipe]: https://developer.apple.com/library/ios/samplecode/iPhoneCoreDataRecipes/Introduction/Intro.html "iPhoneCoreDataRecipes"
[gitrecipe]: https://git.ibmbaas.com/jimix/iphonecoredatarecipes "Git Tree of iPhoneCoreDataRecipes"
[schema]: http://json-schema.org/ "The home of JSON Schema"
[change management]: https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/CoreData/Articles/cdChangeManagement.html "Change Management"
[merge conflict]: https://developer.apple.com/library/mac/documentation/CoreData/Reference/NSMergeConflict_Class/ "NSMergeConflict"
[merge policies]: https://developer.apple.com/library/ios/documentation/CoreData/Reference/NSMergePolicy_Class/ "NSMergePolicy"

<!--  LocalWords:  CDTDatastore CDTIncrementalStore OSX iOS objc BOOL
 -->
<!--  LocalWords:  UIApplicationDelegate UIApplication NSDictionary
 -->
<!--  LocalWords:  didFinishLaunchingWithOptions launchOptions NSURL
 -->
<!--  LocalWords:  storeURL URLWithString databaseURI NSString myType
 -->
<!--  LocalWords:  NSPersistentStoreCoordinator psc NSArray myIS md
 -->
<!--  LocalWords:  addPersistentStoreWithType storesFromCoordinator
 -->
<!--  LocalWords:  firstObject linkReplicators linkURL unlink NSError
 -->
<!--  LocalWords:  unlinkReplicators pushToRemote withProgress PNG zA
 -->
<!--  LocalWords:  pullFromRemote UIProgressView weakProgress pushErr
 -->
<!--  LocalWords:  NSInteger setProgress iPhoneCoreDataRecipes png fA
 -->
<!--  LocalWords:  gitrecipe NSThread MIMEType NSDecimalNumber NaN
 -->
<!--  LocalWords:  JSON tokenized IEEE endian CloudantSync withError
 -->
<!--  LocalWords:  CDTISReplicator replicatorThatPushesToURL isActive
 -->
<!--  LocalWords:  replicatorThatPullsFromURL CDTReplicator Datastore
 -->
<!--  LocalWords:  pullerManager startWithError reportIssue moc alloc
 -->
<!--  LocalWords:  serverVerified NSManagedObjectContext javascript
 -->
<!--  LocalWords:  NSMergeConflict processConflictsWithContext enum
 -->
<!--  LocalWords:  NSMergePolicyClass mergeConflicts NSMergePolicy
 -->
<!--  LocalWords:  initWithMergeType resolveConflicts CDTISMetaData
 -->
<!--  LocalWords:  NSMergeByPropertyStoreTrumpMergePolicyType UTF utf
 -->
<!--  LocalWords:  symbolicName typeName bool xform WoS URI MetaData
 -->
<!--  LocalWords:  versionHash patternProperties objectModel metaData
 -->
<!--  LocalWords:  APPLCoreDataMetaData CDTISObject oneOf CDTISMeta
 -->
<!--  LocalWords:  relationToOne relationToMany metaDataForBinary nan
 -->
<!--  LocalWords:  metaDataForFloatNonFinite uint img num ieee CDTIS
 -->
<!--  LocalWords:  metaDataForFloatSingle nonFinite NSDecimal
 -->
<!--  LocalWords:  metaDataForFloatDouble metaDataForNSDecimal
 -->
<!--  LocalWords:  nsdecimal CDTISEntityName CDTISIdentifier
 -->
<!--  LocalWords:  CDTISObjectVersion
 -->
