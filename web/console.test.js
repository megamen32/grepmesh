"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");
const vm = require("node:vm");

class Element {
  constructor() {
    this.children = [];
    this.dataset = {};
    this.events = new Map();
    this.className = "";
    this.textContent = "";
    this.type = "";
    this.classList = { add() {}, remove() {} };
  }

  get firstElementChild() { return this.children[0] || null; }
  get lastElementChild() { return this.children.at(-1) || null; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  addEventListener(type, listener) { this.events.set(type, listener); }
  click() { this.events.get("click")?.({ preventDefault() {} }); }
  focus() {}
}

function consoleFixture(catalog, browseEntries = []) {
  const ids = ["connection", "search-form", "query", "refresh-catalog", "search-button", "search-state", "host-sidebar", "locations-sidebar", "results", "results-count", "preview", "backup-badge", "backup-body"];
  const nodes = Object.fromEntries(ids.map((id) => [id, new Element()]));
  nodes.connection.append(new Element(), new Element());
  nodes["search-state"].append(new Element(), new Element());
  nodes["search-button"].append(new Element());
  const requests = [];
  const document = {
    getElementById: (id) => nodes[id],
    createElement: () => new Element(),
    querySelectorAll: () => [],
  };
  const fetch = async (url, options = {}) => {
    requests.push({ url, options });
    const body = url === "/api/catalog" ? catalog : url === "/api/backup/availability" ? { state: "unconfigured" } : url === "/api/browse" ? { entries: browseEntries } : { entries: [] };
    return { ok: true, status: 200, text: async () => JSON.stringify(body) };
  };
  vm.runInNewContext(fs.readFileSync("web/console.js", "utf8"), { document, fetch, Error, console });
  return { nodes, requests };
}

async function settle() {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
}

for (const [name, catalog] of [
  ["flat roots", { hosts: [{ id: "server-100" }], roots: ["/etc"] }],
  ["nested roots", { hosts: [{ id: "server-100", roots: ["/etc"] }], roots: ["/etc"] }],
]) {
  test(`Location click browses ${name} catalog and renders its entries`, async () => {
    const { nodes, requests } = consoleFixture(catalog, [
      { name: "nginx", path: "/etc/nginx", kind: "directory" },
      { name: "hosts", path: "/etc/hosts", kind: "file", size: 20 },
    ]);
    await settle();

    const location = nodes["locations-sidebar"].children.find((node) => node.className.includes("location-item"));
    assert.ok(location, "catalog root should render as a clickable Location");
    location.click();
    await settle();

    const browse = requests.find((request) => request.url === "/api/browse");
    assert.ok(browse, "selecting a rendered root must request its directory entries");
    assert.equal(browse.options.method, "POST");
    assert.deepEqual(JSON.parse(browse.options.body), { host: "server-100", path: "/etc" });
    assert.ok(nodes.results.children.some((node) => node.className.includes("browse-entry") && node.children.some((child) => child.textContent === "hosts")), "browse response should replace the central list with directory entries");

    const directory = nodes.results.children.find((node) => node.children.some((child) => child.textContent === "nginx"));
    directory.click();
    await settle();
    assert.ok(requests.some((request) => request.url === "/api/browse" && JSON.parse(request.options.body).path === "/etc/nginx"), "clicking a directory should browse it");

    const file = nodes.results.children.find((node) => node.children.some((child) => child.textContent === "hosts"));
    file.click();
    assert.equal(nodes.preview.className, "preview-content");
    assert.ok(nodes.preview.children.some((child) => child.children.some((detail) => detail.textContent === "/etc/hosts")), "clicking a file should fill Details with its metadata");
  });
}
