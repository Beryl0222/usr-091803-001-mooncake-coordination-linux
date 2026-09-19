"use strict";

const { spawnSync } = require("node:child_process");

// 健康检查契约 + 产销协同领域/HTTP 全套用例。
const result = spawnSync(
  "python3",
  ["-m", "unittest", "-v", "service_contract", "test_coordination"],
  { stdio: "inherit" }
);
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);
