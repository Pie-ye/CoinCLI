#!/usr/bin/env node

import { CopilotClient } from "@github/copilot-sdk";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline/promises";
import process from "node:process";

import { financeTools, TOOL_NAMES } from "./finance-tools.js";

const ALLOWED_TOOLS = new Set(TOOL_NAMES);

const SYSTEM_MESSAGE = `
You are the natural-language finance assistant for CLI-Wealth.

Rules:
- Only use the registered finance tools.
- Never use shell, file, git, web, or any non-finance built-in tool.
- Use tools for ledger mutations, reports, investment trades, dividends, price updates, and portfolio summaries.
- Ask a short clarification question in Traditional Chinese when required fields are missing.
- Reply in concise Traditional Chinese.
- Never invent data that is not returned by tools.
`;

function permissionHandler(request) {
  if (request.kind === "custom-tool" && ALLOWED_TOOLS.has(request.toolName)) {
    return { kind: "approved" };
  }
  return { kind: "denied-by-rules" };
}

function resolveCliPath() {
  if (process.env.COPILOT_CLI_PATH) {
    return process.env.COPILOT_CLI_PATH;
  }

  const candidates = [];

  if (process.platform === "win32") {
    if (process.arch === "x64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-win32-x64", "copilot.exe"));
    }
    if (process.arch === "arm64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-win32-arm64", "copilot.exe"));
    }
    candidates.push(join(process.cwd(), "node_modules", ".bin", "copilot.cmd"));
  } else if (process.platform === "linux") {
    if (process.arch === "x64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-linux-x64", "copilot"));
    }
    if (process.arch === "arm64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-linux-arm64", "copilot"));
    }
    candidates.push(join(process.cwd(), "node_modules", ".bin", "copilot"));
  } else if (process.platform === "darwin") {
    if (process.arch === "x64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-darwin-x64", "copilot"));
    }
    if (process.arch === "arm64") {
      candidates.push(join(process.cwd(), "node_modules", "@github", "copilot-darwin-arm64", "copilot"));
    }
    candidates.push(join(process.cwd(), "node_modules", ".bin", "copilot"));
  }

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return "copilot";
}

async function createAgentSession() {
  const client = new CopilotClient({
    cliPath: resolveCliPath(),
  });

  await client.start();

  const session = await client.createSession({
    model: process.env.COPILOT_MODEL || "gemini-3-flash",
    tools: financeTools,
    onPermissionRequest: permissionHandler,
    systemMessage: {
      content: SYSTEM_MESSAGE,
    },
  });

  return { client, session };
}

async function runSinglePrompt(session, prompt) {
  const response = await session.sendAndWait({ prompt });
  if (response?.data?.content) {
    console.log(response.data.content);
    return;
  }
  console.log("No displayable response was returned.");
}

async function runInteractive(session) {
  const rl = createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  console.log("CLI-Wealth agent is ready. Type natural-language requests, or 'exit' to quit.");

  try {
    while (true) {
      const prompt = (await rl.question("wealth> ")).trim();
      if (!prompt) {
        continue;
      }
      if (["exit", "quit"].includes(prompt.toLowerCase())) {
        break;
      }
      await runSinglePrompt(session, prompt);
    }
  } finally {
    rl.close();
  }
}

async function main() {
  const prompt = process.argv.slice(2).join(" ").trim();

  let client;
  let session;

  try {
    const started = await createAgentSession();
    client = started.client;
    session = started.session;

    if (prompt) {
      await runSinglePrompt(session, prompt);
    } else {
      await runInteractive(session);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("CLI-Wealth agent failed to start.");
    console.error(message);
    console.error("Make sure npm dependencies are installed and GitHub Copilot CLI is authenticated.");
    console.error("If needed, run: npx copilot auth login");
    process.exitCode = 1;
  } finally {
    if (session) {
      await session.disconnect().catch(() => {});
    }
    if (client) {
      await client.stop().catch(() => {});
    }
  }
}

await main();
