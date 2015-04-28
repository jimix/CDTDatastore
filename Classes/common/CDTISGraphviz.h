//
//  CDTISGraphviz.h
//
//
//  Created by Jimi Xenidis on 2/14/15.
//
//

#import <CoreData/CoreData.h>
#import <CloudantSync.h>
#import "CDTIncrementalStore.h"
#import "CDTISObjectModel.h"

/**
 *  CDTISGraphviz creates a graph representation of the datastore using the
 *  [Graphviz](http://www.graphviz.org/) "dot" format.
 *  See the Graphviz docuementation on how to display the output.
 */

@interface CDTISGraphviz : NSObject

/**
 *  This creates the "dot" output
 *
 *  @param datastore The Datastore to graph
 *  @param objectModel The object model that decribes the datastore
 *
 *  @return NSData stucture or `nil` on error
 */
+ (NSData *)dotDatastore:(CDTDatastore *)datastore withObjectModel:(CDTISObjectModel *)objectModel;

@end
