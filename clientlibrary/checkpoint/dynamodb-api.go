/*
 * Copyright (c) 2021 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package checkpoint

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoDBAPI provides an interface to enable mocking the
// dynamodb.DynamoDB service client's API operation,
// paginators, and waiters. This make unit testing your code that calls out
// to the SDK's service client's calls easier.
type DynamoDBAPI interface {
	// The Scan operation returns one or more items and item attributes by accessing
	// every item in a table or a secondary index. To have DynamoDB return fewer items,
	// you can provide a FilterExpression operation. If the total number of scanned
	// items exceeds the maximum dataset size limit of 1 MB, the scan stops and results
	// are returned to the user as a LastEvaluatedKey value to continue the scan in a
	// subsequent operation. The results also include the number of items exceeding the
	// limit. A scan can result in no table data meeting the filter criteria. A single
	// Scan operation reads up to the maximum number of items set (if using the Limit
	// parameter) or a maximum of 1 MB of data and then apply any filtering to the
	// results using FilterExpression. If LastEvaluatedKey is present in the response,
	// you need to paginate the result set.
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)

	// DescribeTable returns information about the table, including the current status of the table,
	// when it was created, the primary key schema, and any indexes on the table. If
	// you issue a DescribeTable request immediately after a CreateTable request,
	// DynamoDB might return a ResourceNotFoundException. This is because DescribeTable
	// uses an eventually consistent query, and the metadata for your table might not
	// be available at that moment. Wait for a few seconds, and then try the
	// DescribeTable request again.
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)

	// The CreateTable operation adds a new table to your account. In an AWS account,
	// table names must be unique within each Region. That is, you can have two tables
	// with same name if you create the tables in different Regions. CreateTable is an
	// asynchronous operation. Upon receiving a CreateTable request, DynamoDB
	// immediately returns a response with a TableStatus of CREATING. After the table
	// is created, DynamoDB sets the TableStatus to ACTIVE. You can perform read and
	// write operations only on an ACTIVE table. You can optionally define secondary
	// indexes on the new table, as part of the CreateTable operation. If you want to
	// create multiple tables with secondary indexes on them, you must create the
	// tables sequentially. Only one table with secondary indexes can be in the
	// CREATING state at any given time. You can use the DescribeTable action to check
	// the table status.
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)

	// PutItem creates a new item, or replaces an old item with a new item. If an item that has
	// the same primary key as the new item already exists in the specified table, the
	// new item completely replaces the existing item. You can perform a conditional
	// put operation (add a new item if one with the specified primary key doesn't
	// exist), or replace an existing item if it has certain attribute values. You can
	// return the item's attribute values in the same operation, using the ReturnValues
	// parameter.
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)

	// The GetItem operation returns a set of attributes for the item with the given
	// primary key. If there is no matching item, GetItem does not return any data and
	// there will be no Item element in the response. GetItem provides an eventually
	// consistent read by default. If your application requires a strongly consistent
	// read, set ConsistentRead to true. Although a strongly consistent read might take
	// more time than an eventually consistent read, it always returns the last updated
	// value.
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)

	// UpdateItem edits an existing item's attributes, or adds a new item to the table if it does
	// not already exist. You can put, delete, or add attribute values. You can also
	// perform a conditional update on an existing item (insert a new attribute
	// name-value pair if it doesn't exist, or replace an existing name-value pair if
	// it has certain expected attribute values). You can also return the item's
	// attribute values in the same UpdateItem operation using the ReturnValues
	// parameter.
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)

	// DeleteItem deletes a single item in a table by primary key. You can perform a conditional
	// delete operation that deletes the item if it exists, or if it has an expected
	// attribute value. In addition to deleting an item, you can also return the item's
	// attribute values in the same operation, using the ReturnValues parameter. Unless
	// you specify conditions, the DeleteItem is an idempotent operation; running it
	// multiple times on the same item or attribute does not result in an error
	// response. Conditional deletes are useful for deleting items only if specific
	// conditions are met. If those conditions are met, DynamoDB performs the delete.
	// Otherwise, the item is not deleted.
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}
