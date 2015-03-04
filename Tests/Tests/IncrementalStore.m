//
//  IncrementalStore.m
//  Tests
//
//  Created by Jimi Xenidis on 12/7/14.
//
//

#import <CoreData/CoreData.h>
#import <XCTest/XCTest.h>

#import "CloudantSyncTests.h"
#import "CDTIncrementalStore.h"

#ifdef CDTDATASTORE_SUPPORTS_COMPOUND_PREDICATES
static BOOL SupportCompoundPredicates = YES;
#else
static BOOL SupportCompoundPredicates = NO;
#endif

/*
 *  ##Start Ripoff:
 *  The following code segment, that creates a managed object model
 *  programmatically, has been derived from:
 *  >
 *https://github.com/couchbase/couchbase-lite-ios/blob/master/Source/API/Extras/CBLIncrementalStoreTests.m
 *
 *  Which at the time of pilferage had the following license:
 *  > http://www.apache.org/licenses/LICENSE-2.0
 */

@interface Entry : NSManagedObject
@property (nonatomic, retain) NSNumber *check;
@property (nonatomic, retain) NSDate *created_at;
@property (nonatomic, retain) NSString *text;
@property (nonatomic, retain) NSString *text2;
@property (nonatomic, retain) NSNumber *i16;
@property (nonatomic, retain) NSNumber *i32;
@property (nonatomic, retain) NSNumber *i64;
@property (nonatomic, retain) NSDecimalNumber *fpDecimal;
@property (nonatomic, retain) NSNumber *fpDouble;
@property (nonatomic, retain) NSNumber *fpFloat;
@property (nonatomic, retain) NSSet *subEntries;
@property (nonatomic, retain) NSSet *files;
@end

@class Subentry;
@class File;

@interface Entry (CoreDataGeneratedAccessors)
- (void)addSubEntriesObject:(Subentry *)value;
- (void)removeSubEntriesObject:(Subentry *)value;
- (void)addSubEntries:(NSSet *)values;
- (void)removeSubEntries:(NSSet *)values;

- (void)addFilesObject:(File *)value;
- (void)removeFilesObject:(File *)value;
- (void)addFiles:(NSSet *)values;
- (void)removeFiles:(NSSet *)values;
@end

@interface Subentry : NSManagedObject
@property (nonatomic, retain) NSString *text;
@property (nonatomic, retain) NSNumber *number;
@property (nonatomic, retain) Entry *entry;
@end

@interface File : NSManagedObject
@property (nonatomic, retain) NSString *filename;
@property (nonatomic, retain) NSData *data;
@property (nonatomic, retain) Entry *entry;
@end

NSAttributeDescription *MakeAttribute(NSString *name, BOOL optional, NSAttributeType type,
                                      id defaultValue)
{
    NSAttributeDescription *attribute = [NSAttributeDescription new];
    [attribute setName:name];
    [attribute setOptional:optional];
    [attribute setAttributeType:type];
    if (defaultValue) {
        [attribute setDefaultValue:defaultValue];
    }
    return attribute;
}

NSRelationshipDescription *MakeRelationship(NSString *name, BOOL optional, BOOL toMany,
                                            NSDeleteRule deletionRule,
                                            NSEntityDescription *destinationEntity)
{
    NSRelationshipDescription *relationship = [NSRelationshipDescription new];
    [relationship setName:name];
    [relationship setOptional:optional];
    [relationship setMinCount:optional ? 0 : 1];
    [relationship setMaxCount:toMany ? 0 : 1];
    [relationship setDeleteRule:deletionRule];
    [relationship setDestinationEntity:destinationEntity];
    return relationship;
}

NSManagedObjectModel *MakeCoreDataModel(void)
{
    NSManagedObjectModel *model = [NSManagedObjectModel new];

    NSEntityDescription *entry = [NSEntityDescription new];
    [entry setName:@"Entry"];
    [entry setManagedObjectClassName:@"Entry"];

    NSEntityDescription *file = [NSEntityDescription new];
    [file setName:@"File"];
    [file setManagedObjectClassName:@"File"];

    NSEntityDescription *subentry = [NSEntityDescription new];
    [subentry setName:@"Subentry"];
    [subentry setManagedObjectClassName:@"Subentry"];

    NSRelationshipDescription *entryFiles =
        MakeRelationship(@"files", YES, YES, NSCascadeDeleteRule, file);
    NSRelationshipDescription *entrySubentries =
        MakeRelationship(@"subEntries", YES, YES, NSCascadeDeleteRule, subentry);
    NSRelationshipDescription *fileEntry =
        MakeRelationship(@"entry", YES, NO, NSNullifyDeleteRule, entry);
    NSRelationshipDescription *subentryEntry =
        MakeRelationship(@"entry", YES, NO, NSNullifyDeleteRule, entry);

    [entryFiles setInverseRelationship:fileEntry];
    [entrySubentries setInverseRelationship:subentryEntry];
    [fileEntry setInverseRelationship:entryFiles];
    [subentryEntry setInverseRelationship:entrySubentries];

    [entry setProperties:@[
        MakeAttribute(@"check", YES, NSBooleanAttributeType, nil),
        MakeAttribute(@"created_at", YES, NSDateAttributeType, nil),
        MakeAttribute(@"fpDecimal", YES, NSDecimalAttributeType, @(0.0)),
        MakeAttribute(@"fpDouble", YES, NSDoubleAttributeType, @(0.0)),
        MakeAttribute(@"fpFloat", YES, NSFloatAttributeType, @(0.0)),
        MakeAttribute(@"i16", YES, NSInteger16AttributeType, @(0)),
        MakeAttribute(@"i32", YES, NSInteger32AttributeType, @(0)),
        MakeAttribute(@"i64", YES, NSInteger64AttributeType, @(0)),
        MakeAttribute(@"text", YES, NSStringAttributeType, nil),
        MakeAttribute(@"text2", YES, NSStringAttributeType, nil),
        entryFiles,
        entrySubentries
    ]];

    [file setProperties:@[
        MakeAttribute(@"data", YES, NSBinaryDataAttributeType, nil),
        MakeAttribute(@"filename", YES, NSStringAttributeType, nil),
        fileEntry
    ]];

    [subentry setProperties:@[
        MakeAttribute(@"number", YES, NSInteger32AttributeType, @(0)),
        MakeAttribute(@"text", YES, NSStringAttributeType, nil),
        subentryEntry
    ]];

    [model setEntities:@[ entry, file, subentry ]];

    return model;
}

@implementation Entry
@dynamic check, created_at, text, text2, i16, i32, i64, fpDecimal, fpDouble, fpFloat, subEntries,
    files;
@end

@implementation Subentry
@dynamic text, number, entry;
@end

@implementation File
@dynamic filename, data, entry;
@end

/*
 *  ##End Ripoff:
 */

Entry *MakeEntry(NSManagedObjectContext *moc)
{
    return
        [NSEntityDescription insertNewObjectForEntityForName:@"Entry" inManagedObjectContext:moc];
}

@interface IncrementalStore : CloudantSyncTests

@property (nonatomic, strong) NSManagedObjectModel *managedObjectModel;
@property (nonatomic, strong) NSManagedObjectContext *managedObjectContext;
@property (nonatomic, strong) NSPersistentStoreCoordinator *persistentStoreCoordinator;

@end

@implementation IncrementalStore

static void *ISContextProgress = &ISContextProgress;

- (void)observeValueForKeyPath:(NSString *)keyPath
                      ofObject:(id)object
                        change:(NSDictionary *)change
                       context:(void *)context
{
    if (context == ISContextProgress) {
        NSProgress *progress = object;
        NSLog(@"Progress: %@ / %@", @(progress.completedUnitCount), @(progress.totalUnitCount));
    } else {
        [super observeValueForKeyPath:keyPath ofObject:object change:change context:context];
    }
}

- (NSManagedObjectModel *)managedObjectModel
{
    if (_managedObjectModel != nil) {
        return _managedObjectModel;
    }
    _managedObjectModel = MakeCoreDataModel();

    return _managedObjectModel;
}

- (NSManagedObjectContext *)managedObjectContext
{
    if (_managedObjectContext != nil) {
        return _managedObjectContext;
    }

    NSPersistentStoreCoordinator *coordinator = [self persistentStoreCoordinator];
    if (coordinator != nil) {
        _managedObjectContext = [NSManagedObjectContext new];
        [_managedObjectContext setPersistentStoreCoordinator:coordinator];
    }
    return _managedObjectContext;
}

- (NSPersistentStoreCoordinator *)persistentStoreCoordinator
{
    if (_persistentStoreCoordinator != nil) {
        return _persistentStoreCoordinator;
    }
    _persistentStoreCoordinator =
        [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:[self managedObjectModel]];

    NSError *err = nil;
    NSURL *storeURL = [NSURL URLWithString:@"cdtis_test"];
    NSPersistentStore *theStore;
    theStore = [_persistentStoreCoordinator addPersistentStoreWithType:[CDTIncrementalStore type]
                                                         configuration:nil
                                                                   URL:storeURL
                                                               options:nil
                                                                 error:&err];
    XCTAssertNotNil(theStore, @"could not get theStore: %@", err);
    return _persistentStoreCoordinator;
}

- (void)setUp
{
    [super setUp];

    static BOOL initialized = NO;
    if (!initialized) {
        [CDTIncrementalStore initialize];
        initialized = YES;
    }

    // remove the entire database directory
    NSError *err = nil;
    NSURL *dir = [CDTIncrementalStore localDir];
    NSFileManager *fm = [NSFileManager defaultManager];
    if (![fm removeItemAtURL:dir error:&err]) {
        if (err.code != NSFileNoSuchFileError) {
            XCTAssertNil(err, @"%@", err);
        }
    }
}

- (void)tearDown
{
    self.managedObjectContext = nil;
    self.persistentStoreCoordinator = nil;

    [super tearDown];
}

- (void)testAsyncFetch
{
    int max = 5000;
    NSUInteger __block completed = 0;

    NSError *err = nil;
    // This will create the database and wire everything up
    NSManagedObjectContext *moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    for (int i = 0; i < max; i++) {
        Entry *e = MakeEntry(moc);
        // check will indicate if value is an even number
        e.check = (i % 2) ? @NO : @YES;
        e.i64 = @(i);
        e.fpFloat = @(((float)(M_PI)) * (float)i);
        e.text = [NSString stringWithFormat:@"%u", (max * 10) + i];

        if ((i % (max / 10)) == 0) {
            NSLog(@"Saving %u of %u", i, max);
            XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);
        }
    }
    NSLog(@"Saving %u of %u", max, max);
    XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);


    // create other context that will fetch from our store
    NSManagedObjectContext *otherMOC =
        [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
    XCTAssertNotNil(otherMOC, @"could not create Context");
    [otherMOC setPersistentStoreCoordinator:self.persistentStoreCoordinator];

    NSPredicate *even = [NSPredicate predicateWithFormat:@"check == YES"];
    NSSortDescriptor *sd = [NSSortDescriptor sortDescriptorWithKey:@"i64" ascending:YES];
    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;
    fr.predicate = even;
    fr.sortDescriptors = @[ sd ];
    // this does not do anything, but maybe it will one day
    fr.fetchBatchSize = 10;

    NSAsynchronousFetchRequest *asyncFetch = [[NSAsynchronousFetchRequest alloc]
        initWithFetchRequest:fr
             completionBlock:^(NSAsynchronousFetchResult *result) {
                 NSLog(@"Final: %@", @(result.finalResult.count));
                 [result.progress removeObserver:self
                                      forKeyPath:@"completedUnitCount"
                                         context:ISContextProgress];
                 [result.progress removeObserver:self
                                      forKeyPath:@"totalUnitCount"
                                         context:ISContextProgress];
                 completed = result.finalResult.count;
             }];

    [otherMOC performBlock:^{
        // Create Progress
        NSProgress *progress = [NSProgress progressWithTotalUnitCount:1];

        // Become Current
        [progress becomeCurrentWithPendingUnitCount:1];

        // Execute Asynchronous Fetch Request
        NSError *err = nil;
        NSAsynchronousFetchResult *asyncFetchResult =
            (NSAsynchronousFetchResult *)[otherMOC executeRequest:asyncFetch error:&err];

        if (err) {
            NSLog(@"Unable to execute asynchronous fetch result: %@", err);
        }

        // Add Observer
        [asyncFetchResult.progress addObserver:self
                                    forKeyPath:@"completedUnitCount"
                                       options:NSKeyValueObservingOptionNew
                                       context:ISContextProgress];
        [asyncFetchResult.progress addObserver:self
                                    forKeyPath:@"totalUnitCount"
                                       options:NSKeyValueObservingOptionNew
                                       context:ISContextProgress];
        // Resign Current
        [progress resignCurrent];


    }];

    while (completed == 0) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    XCTAssertTrue(completed == max / 2, @"completed should be %@ is %@", @(completed), @(max));
}

- (void)testPredicates
{
    int max = 100;

    XCTAssertTrue((max % 4) == 0, @"Test assumes max is mod 4: %d", max);

    NSError *err = nil;
    // This will create the database
    NSManagedObjectContext *moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    for (int i = 0; i < max; i++) {
        Entry *e = MakeEntry(moc);
        // check will indicate if value is an even number
        e.check = (i % 2) ? @NO : @YES;
        e.i64 = @(i);
        e.fpFloat = @(((float)(M_PI)) * (float)i);
        e.text = [NSString stringWithFormat:@"%u", (max * 10) + i];
    }

    // push it out
    XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);

    NSArray *results;
    /**
     *  Fetch even numbers
     */
    NSPredicate *even = [NSPredicate predicateWithFormat:@"check == YES"];

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;
    fr.predicate = even;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == (max / 2), @"results count should be %d is %@", max / 2,
                  @([results count]));

    for (Entry *e in results) {
        XCTAssertTrue([e.check boolValue], @"not even?");

        long long val = [e.i64 longLongValue];
        XCTAssertTrue((val % 2) == 0, @"entry.i64 should be even");
    }

    /**
     *  Fetch odd Numbers
     */
    NSPredicate *odd = [NSPredicate predicateWithFormat:@"check == NO"];
    fr.predicate = odd;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == (max / 2), @"results count should be %d is %@", max / 2,
                  @([results count]));

    for (Entry *e in results) {
        XCTAssertFalse([e.check boolValue], @"not odd?");

        long long val = [e.i64 longLongValue];
        XCTAssertTrue((val % 2) == 1, @"entry.i64 should be odd");
    }

    /**
     *  fetch == value
     *  * `{index: value}`: index == value
     */
    NSPredicate *eq = [NSPredicate predicateWithFormat:@"i64 == %u", max / 2];
    fr.predicate = eq;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == 1, @"results count should be %d is %@", max / 2,
                  @([results count]));

    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val == (max / 2), @"entry.i64 should be %d is %lld", max / 2, val);
    }

    /**
     *  fetch <= value
     *  * `{index: @{@"max": value}}`: index <= value
     */
    NSPredicate *lte = [NSPredicate predicateWithFormat:@"i64 <= %u", max / 2];
    fr.predicate = lte;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == ((max / 2) + 1), @"results count should be %d is %@",
                  (max / 2) + 1, @([results count]));

    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val <= (max / 2), @"entry.i64 should be <= %d, is %lld", max / 2, val);
    }

    /**
     *  fetch >= value
     *  * `{index: @{@"min": value}}`: index >= value
     */
    NSPredicate *gte = [NSPredicate predicateWithFormat:@"i64 >= %u", max / 2];
    fr.predicate = gte;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == (max / 2), @"results count should be %d is %@", max / 2,
                  @([results count]));

    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val >= (max / 2), @"entry.i64 should be >= %d, is %lld", max / 2, val);
    }

    /**
     *  strict "less than" not supported
     */
    NSPredicate *nosup = [NSPredicate predicateWithFormat:@"i64 < %u", max];
    fr.predicate = nosup;

    XCTAssertThrowsSpecificNamed([moc executeFetchRequest:fr error:&err], NSException,
                                 CDTISException, @"Expected Exception");

    /**
     *  fetch in range
     *  * `{index: @{@"min": value1, @"max": value2}}`: value1 <= index <= value2
     */
    int start = max / 4;
    int end = (max * 3) / 4;
    NSPredicate *tween = [NSPredicate predicateWithFormat:@"i64 between { %u, %u }", start, end];
    fr.predicate = tween;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == (max / 2) + 1, @"results count should be %d is %@", max / 2,
                  @([results count]));

    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val >= start && val <= end, @"entry.i64 should be between [%d, %d] is %lld",
                      start, end, val);
    }

    /**
     *  fetch member of
     *  * `{index: @[value_0,...,value_n]}`: index == value_0 || ... || index == value_n
     */
    NSComparisonPredicate *cp;
    NSExpression *lhs;
    NSExpression *rhs;

    // make a set of random numbers in set
    NSMutableSet *nums = [NSMutableSet set];
    for (int i = 0; i < max / 4; i++) {
        uint32_t r = arc4random();
        r %= max;
        [nums addObject:@(r)];
    }
    NSUInteger count = [nums count];
    // add one that is not there for dun
    [nums addObject:@(max)];

    lhs = [NSExpression expressionForKeyPath:@"i64"];
    rhs = [NSExpression expressionForConstantValue:[nums allObjects]];
    cp = [NSComparisonPredicate predicateWithLeftExpression:lhs
                                            rightExpression:rhs
                                                   modifier:NSDirectPredicateModifier
                                                       type:NSInPredicateOperatorType
                                                    options:0];
    fr.predicate = cp;
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == count, @"results count should be %@ is %@", @(count),
                  @([results count]));

    for (Entry *e in results) {
        XCTAssertTrue([nums containsObject:e.i64], @"entry.i64: %@ should be in set", e.i64);
    }

    /**
     *  fetch objects from a list of objects
     */
    // we will borrow the results from the test above
    NSMutableSet *ids = [NSMutableSet set];
    for (Entry *e in results) {
        NSManagedObjectID *moid = e.objectID;
        NSURL *uri = [moid URIRepresentation];
        NSString *s = [uri absoluteString];
        [ids addObject:s];
    }

    lhs = [NSExpression expressionForEvaluatedObject];
    rhs = [NSExpression expressionForConstantValue:[ids allObjects]];
    cp = [NSComparisonPredicate predicateWithLeftExpression:lhs
                                            rightExpression:rhs
                                                   modifier:NSDirectPredicateModifier
                                                       type:NSInPredicateOperatorType
                                                    options:0];
    fr.predicate = cp;
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == count, @"results count should be %@ is %@", @(count),
                  @([results count]));

    for (Entry *e in results) {
        XCTAssertTrue([nums containsObject:e.i64], @"entry.i64: %@ should be in set", e.i64);
    }

    /**
     *  No support for substring "in" predicated
     */
    lhs = [NSExpression expressionForKeyPath:@"text"];
    rhs = [NSExpression expressionForConstantValue:@"0"];
    cp = [NSComparisonPredicate predicateWithLeftExpression:lhs
                                            rightExpression:rhs
                                                   modifier:NSDirectPredicateModifier
                                                       type:NSInPredicateOperatorType
                                                    options:0];
    fr.predicate = cp;

    XCTAssertThrowsSpecificNamed([moc executeFetchRequest:fr error:&err], NSException,
                                 CDTISException, @"Expected Exception");

    /**
     *  Compound Predicates
     */
    if (!SupportCompoundPredicates) {
        NSPredicate *both = [NSPredicate predicateWithFormat:@"check == NO || check == YES"];
        fr.predicate = both;

        XCTAssertThrowsSpecificNamed([moc executeFetchRequest:fr error:&err], NSException,
                                     CDTISException, @"Expected Exception");
    } else {
        /**
         *  fetch both with or
         */
        NSPredicate *both = [NSPredicate predicateWithFormat:@"check == NO || check == YES"];
        fr.predicate = both;

        results = [moc executeFetchRequest:fr error:&err];
        XCTAssertNotNil(results, @"Expected results: %@", err);

        XCTAssertTrue([results count] == max, @"results count should be %d is %@", max,
                      @([results count]));

        /**
         *  Fetch none with AND, yes I know this is nonsense
         */
        NSPredicate *none = [NSPredicate predicateWithFormat:@"check == NO && check == YES"];
        fr.predicate = none;

        results = [moc executeFetchRequest:fr error:&err];
        XCTAssertNotNil(results, @"Expected results: %@", err);

        XCTAssertTrue([results count] == 0, @"results count should be %d is %@", 0,
                      @([results count]));
    }
    /**
     *  test predicates with Floats see if NaN shows up
     */
    NSPredicate *floatPi = [NSPredicate predicateWithFormat:@"fpFloat <= %f", M_PI * 2];
    fr.predicate = floatPi;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == 3, @"results count should be %d is %@", 3, @([results count]));

    // make one of them NaN
    Entry *nan = [results firstObject];
    nan.fpFloat = @((float)NAN);

    // push it out
    XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == 2, @"results count should be %d is %@", 2, @([results count]));
}

- (void)testFetchConstraints
{
    int max = 100;
    int limit = 10;
    int offset = 50;

    XCTAssertTrue(offset + limit <= max && offset - limit >= 0,
                  @"test parameters out of legal range");

    NSError *err = nil;
    // This will create the database
    NSManagedObjectContext *moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    for (int i = 0; i < max; i++) {
        Entry *e = MakeEntry(moc);
        e.i64 = @(i);
        e.text = [NSString stringWithFormat:@"%u", (max * 10) + i];
    }

    // push it out
    XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);

    NSArray *results;
    /**
     *  We will sort by number first
     */
    NSSortDescriptor *sd = [NSSortDescriptor sortDescriptorWithKey:@"i64" ascending:YES];

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;
    fr.sortDescriptors = @[ sd ];
    fr.fetchLimit = limit;
    fr.fetchOffset = offset;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == limit, @"results count should be %d is %@", limit,
                  @([results count]));
    long long last = offset - 1;
    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val >= offset && val < offset + limit,
                      @"entry is out of range [%d, %d): %lld", offset, offset + limit, val);
        XCTAssertTrue(val == last + 1, @"unexpected entry %@: %@", @(val), e);
        ++last;
    }

    /**
     *  now by string, descending just for fun
     */
    sd = [NSSortDescriptor sortDescriptorWithKey:@"text" ascending:NO];

    fr.sortDescriptors = @[ sd ];
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);

    XCTAssertTrue([results count] == limit, @"results count should be %d is %@", limit,
                  @([results count]));
    last = offset;
    for (Entry *e in results) {
        long long val = [e.i64 longLongValue];
        XCTAssertTrue(val >= offset - limit && val < offset,
                      @"entry is out of range [%d, %d): %lld", offset - limit, offset, val);
        XCTAssertTrue(val == last - 1, @"unexpected entry %@: %@", @(val), e);
        --last;
    }
}

- (void)testCheckNumbers
{
    NSError *err = nil;
    // This will create the database
    NSManagedObjectContext *moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    // Increment this for every entry you make
    NSUInteger entries = 0;

    Entry *maxNums = MakeEntry(moc);
    ++entries;
    maxNums.text = @"maximums";
    maxNums.check = @YES;
    maxNums.fpDecimal = [NSDecimalNumber maximumDecimalNumber];
    maxNums.fpDouble = @(DBL_MAX);
    maxNums.fpFloat = @(FLT_MAX);
    maxNums.i16 = @INT16_MAX;
    maxNums.i32 = @INT32_MAX;
    maxNums.i64 = @INT64_MAX;

    Entry *minNums = MakeEntry(moc);
    ++entries;
    minNums.text = @"minimums";
    minNums.check = @NO;
    minNums.fpDecimal = [NSDecimalNumber minimumDecimalNumber];
    minNums.fpDouble = @(DBL_MIN);
    minNums.fpFloat = @(FLT_MIN);
    minNums.i16 = @INT16_MIN;
    minNums.i32 = @INT32_MIN;
    minNums.i64 = @INT64_MIN;

    Entry *infNums = MakeEntry(moc);
    ++entries;
    infNums.text = @"INFINITY";
    infNums.fpDouble = @(INFINITY);
    infNums.fpFloat = @(INFINITY);
    infNums.fpDecimal = (NSDecimalNumber *)[NSDecimalNumber numberWithDouble:INFINITY];

    Entry *ninfNums = MakeEntry(moc);
    ++entries;
    ninfNums.text = @"-INFINITY";
    ninfNums.fpFloat = @(-INFINITY);
    ninfNums.fpDouble = @(-INFINITY);
    ninfNums.fpDecimal = (NSDecimalNumber *)[NSDecimalNumber numberWithDouble:-INFINITY];

    Entry *nanNums = MakeEntry(moc);
    ++entries;
    nanNums.text = @"NaN";
    nanNums.fpDouble = @(NAN);
    nanNums.fpFloat = @(NAN);
    nanNums.fpDecimal = [NSDecimalNumber notANumber];

    XCTAssertTrue([moc save:&err], @"Save Failed: %@", err);

    // does this really cause everything to fault?
    [moc refreshObject:maxNums mergeChanges:NO];
    [moc refreshObject:minNums mergeChanges:NO];
    [moc refreshObject:infNums mergeChanges:NO];
    [moc refreshObject:nanNums mergeChanges:NO];

    XCTAssertTrue([maxNums.fpDouble isEqualToNumber:@(DBL_MAX)], @"Failed to retain double max");
    XCTAssertTrue([minNums.fpDouble isEqualToNumber:@(DBL_MIN)], @"Failed to retain double min");

    XCTAssertTrue([maxNums.fpFloat isEqualToNumber:@(FLT_MAX)], @"Failed to retain float max");
    XCTAssertTrue([minNums.fpFloat isEqualToNumber:@(FLT_MIN)], @"Failed to retain float min");

    XCTAssertTrue([maxNums.fpDecimal isEqual:[NSDecimalNumber maximumDecimalNumber]],
                  @"Failed to retain decimal max");
    XCTAssertTrue([minNums.fpDecimal isEqual:[NSDecimalNumber minimumDecimalNumber]],
                  @"Failed to retain decimal min");

    XCTAssertTrue([infNums.fpDouble isEqualToNumber:@(INFINITY)],
                  @"Failed to retain double infinity");
    XCTAssertTrue([infNums.fpFloat isEqualToNumber:@(INFINITY)],
                  @"Failed to retain float infinity");
    XCTAssertTrue(
        [infNums.fpDecimal isEqual:(NSDecimalNumber *)[NSDecimalNumber numberWithDouble:INFINITY]],
        @"Failed to retain decimal infinity");

    XCTAssertTrue([nanNums.fpDouble isEqualToNumber:@(NAN)], @"Failed to retain double NaN");
    XCTAssertTrue([nanNums.fpFloat isEqualToNumber:@(NAN)], @"Failed to retain float NaN");
    XCTAssertTrue([nanNums.fpDecimal isEqual:[NSDecimalNumber notANumber]],
                  @"Failed to retain decimal NaN");

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    NSUInteger count = [moc countForFetchRequest:fr error:&err];

    XCTAssertTrue(count == entries, @"Count fails");
}

#if 0
- (void)testPerformanceExample {
    // This is an example of a performance test case.
    [self measureBlock:^{
        // Put the code you want to measure the time of here.
    }];
}
#endif

@end
