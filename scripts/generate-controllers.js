#!/usr/bin/env node

import fs from 'fs';
import path from 'path';

import YAML from 'yaml';

// Read the YAML file
const yamlContent = fs.readFileSync('./api/openapi.yaml', 'utf8');
const apiSpec = YAML.parse(yamlContent);

// Base directory for controllers
const controllersDir = './src/controllers';

// Create directory if it doesn't exist
if (!fs.existsSync(controllersDir)) {
  fs.mkdirSync(controllersDir, { recursive: true });
}

// Store controller information
const controllers = new Map();

// Process paths
Object.entries(apiSpec.paths).forEach(([pathUrl, methods]) => {
  Object.entries(methods).forEach(([method, operation]) => {
    if (operation['x-eov-operation-handler'] && operation['x-eov-operation-id']) {
      const handlerName = operation['x-eov-operation-handler'];
      const operationId = operation['x-eov-operation-id'];

      if (!controllers.has(handlerName)) {
        controllers.set(handlerName, []);
      }

      controllers.get(handlerName).push({
        operationId,
        method: method.toUpperCase(),
        path: pathUrl,
        summary: operation.summary || '',
        description: operation.description || '',
      });
    }
  });
});

// Generate controller files
controllers.forEach((operations, controllerName) => {
  const fileName = `${controllerName}.ts`;
  const filePath = path.join(controllersDir, fileName);

  // Check if file already exists
  if (fs.existsSync(filePath)) {
    console.log(`⚠️  ${fileName} already exists, skipping...`);
    return;
  }

  // Generate controller content. No try/catch: Express 5 forwards a rejected
  // async handler to the error middleware itself. Every operation in this API
  // answers 200 — mutations are POSTs that return the committed result, and
  // DELETE is forbidden by design (see CLAUDE.md).
  let content = `/**
 * ${controllerName}
 * Auto-generated from OpenAPI specification
 */

import type { ApiRequest, ApiResponse } from '../types/api-helpers';

`;

  operations.forEach((op) => {
    content += `/**
 * ${op.summary}
 * ${op.description}
 * @route ${op.method} ${op.path}
 */
export const ${op.operationId} = async (
  req: ApiRequest<'${op.operationId}'>,
  res: ApiResponse<'${op.operationId}'>,
): Promise<void> => {
  // TODO: Implement business logic
  // Type information:
  // - req.params: Typed path parameters
  // - req.query: Typed query parameters
  // - req.body: Typed request body

  // TODO: Return properly typed response matching the schema
  throw new Error('${op.operationId} not implemented');
};

`;
  });

  // Write file
  fs.writeFileSync(filePath, content);
  console.log(`✅ Generated: ${fileName}`);
  console.log(`   Operations: ${operations.map((o) => o.operationId).join(', ')}`);
});

console.log('\n🎉 Generation completed!');
