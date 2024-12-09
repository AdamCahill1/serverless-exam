import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { MovieCrewRole } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv();
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieCrewRole"] || {}
);

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", event);
    const parameters = event?.pathParameters;
    const role = parameters?.role;
    const movieId = parameters?.movieId? parseInt(parameters.movieId) : undefined;
    // if (!role) {
    //   return {
    //     statusCode: 404,
    //     headers: {
    //       "content-type": "application/json",
    //     },
    //     body: JSON.stringify({ parameters }),
    //   };
    // }
    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id" }),
      };
    }

    const queryParams = event.queryStringParameters;
    if (queryParams && !isValidQueryParams(queryParams)) {
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          message: `Incorrect type. Must match Query parameters schema`,
          schema: schema.definitions["MovieCrewRole"],
        }),
      };
    }
    let commandInput: QueryCommandInput = {
      TableName: process.env.TABLE_NAME,
    };
    if (queryParams) {
       if ("name" in queryParams) {
        commandInput = {
          ...commandInput,
          KeyConditionExpression:
            "movieId = :m and begins_with(crewName, :c) ",
          ExpressionAttributeValues: {
            ":m": movieId,
            ":c": queryParams.name,
          },
        };
      }
    } else {
      commandInput = {
        ...commandInput,
        KeyConditionExpression: "movieId = :m and begins_with(crewRole, :c) ",
        ExpressionAttributeValues: {
          ":m": movieId,
          ":c": role,
        },
      };
    }

    const commandOutput = await ddbDocClient.send(
      new QueryCommand(commandInput)
    );

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        data: commandOutput.Items,
      }),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
