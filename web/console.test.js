"use strict";
const assert=require("node:assert/strict"),fs=require("node:fs"),test=require("node:test"),vm=require("node:vm");
class Element {
  constructor(){this.children=[];this.dataset={};this.events=new Map();this.className="";this.textContent="";this.attributes={};this.hidden=false;this.classList={add:name=>{this.className+=" "+name;},remove:name=>{this.className=this.className.split(" ").filter(x=>x!==name).join(" ");},toggle:name=>{this.className.includes(name)?this.classList.remove(name):this.classList.add(name);}};}
  get firstElementChild(){return this.children[0];}get lastElementChild(){return this.children.at(-1);}
  append(...children){children.forEach(child=>child.parentElement=this);this.children.push(...children);}
  replaceChildren(...children){this.children=[];this.append(...children);}
  addEventListener(name,listener){this.events.set(name,listener);}setAttribute(key,value){this.attributes[key]=value;}
  dispatch(name,target=this,key){return this.events.get(name)?.({target,key,preventDefault(){}});}click(){return this.dispatch("click");}
  closest(selector){for(let node=this;node;node=node.parentElement)if(node.className.split(" ").includes(selector.slice(1)))return node;return null;}
  querySelectorAll(selector){const found=[];const visit=node=>{for(const child of node.children){if(selector==="details"&&child.tagName==="DETAILS")found.push(child);visit(child);}};visit(this);return found;}
  contains(candidate){for(let node=candidate;node;node=node.parentElement)if(node===this)return true;return false;}focus(){}
}
const entries=[{name:"zeta.txt",path:"/etc/zeta.txt",kind:"file",size:100,modified_ms:3000,created_ms:null},{name:"alpha.txt",path:"/etc/alpha.txt",kind:"file",size:3,modified_ms:1000,created_ms:2000},{name:"folder",path:"/etc/folder",kind:"directory",size:null,modified_ms:2000,created_ms:1000}];
function fixture(options={}){
  const ids=[...fs.readFileSync("web/index.html","utf8").matchAll(/id="([^"]+)"/g)].map(x=>x[1]);const nodes=Object.fromEntries(ids.map(id=>[id,new Element()]));
  nodes.connection.append(new Element(),new Element());nodes["search-state"].append(new Element(),new Element());nodes["search-button"].append(new Element());
  const sorts=["name","size","modified","created"].map(key=>{const node=new Element();node.dataset.sort=key;node.append(new Element());return node;});
  const storage=options.storage || new Map(),requests=[],window=new Element();
  const document={hidden:false,getElementById:id=>nodes[id],createElement:name=>{const node=new Element();node.tagName=String(name).toUpperCase();return node;},querySelector:()=>window,querySelectorAll:selector=>selector==="[data-sort]"?sorts:selector===".result.selected"?nodes.results.children.filter(node=>node.className.includes("result")&&node.className.includes("selected")):[]};
  const catalog=options.catalog || {hosts:[{id:"server-100",roots:["/etc"],health:"healthy"}],roots:["/etc"]};
  const responses={"/api/catalog":catalog,"/api/browse":{entries},"/api/host-status":{nodes:[{host_id:"server-100",roots:["/etc"],index_state:"Ready",index_db_bytes:32768,indexed_files:3}]},"/api/telemetry":{records:[{job_id:"j1",query:"needle",started_ms:1000,duration_ms:15,status:"complete",result_count:2,host_timings:[{host_id:"server-100",duration_ms:12,status:"complete",result_count:2}]}],retention:{max_records:500,max_age_days:30}},"/api/search":{state:"complete",results:[]},"/api/backup/availability":{state:"unconfigured"},...options.responses};
  if(options.catalog && !options.responses?.["/api/host-status"])responses["/api/host-status"]={nodes:options.catalog.hosts.filter(host=>host.health!=="offline").map(host=>({host_id:host.id,roots:(host.roots || []).map(root=>typeof root==="string"?root:root.path),index_state:"Ready",index_db_bytes:32768,indexed_files:3}))};
  const fetch=async(url,opts={})=>{requests.push({url,options:opts});const body=typeof responses[url]==="function"?await responses[url](opts):responses[url] || {};return{ok:true,status:200,text:async()=>JSON.stringify(body)};};
  let interval;
  const markdownit=options=>({options,render:source=>`<h1>${source.replace(/^#\s*/gm,"").replace(/</g,"&lt;").replace(/>/g,"&gt;")}</h1>`});
  vm.runInNewContext(fs.readFileSync("web/console.js","utf8"),{document,fetch,Error,console,markdownit,setTimeout,setInterval:callback=>{interval=callback;},localStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value)}});
  return{nodes,sorts,requests,storage,pollStatus:()=>interval()};
}
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
function content(node){return[node.textContent,...node.children.map(content)].join(" ");}
function rows(f){return f.nodes.results.children.filter(x=>x.className.includes("file-row"));}
function names(f){return rows(f).map(row=>row.children[0].children[1].textContent);}
function descendants(node){return[node,...node.children.flatMap(descendants)];}
async function browse(f){await settle();const root=f.nodes["locations-sidebar"].children[0];f.nodes["locations-sidebar"].dispatch("click",root.children[1]);await settle();}
test("nested location click browses its host; sorting covers the full list and unknown dates stay last",async()=>{
  const f=fixture();await browse(f);assert.deepEqual(JSON.parse(f.requests.find(x=>x.url==="/api/browse").options.body),{host:"server-100",path:"/etc"});assert.deepEqual(names(f),["alpha.txt","folder","zeta.txt"]);
  f.sorts[1].click();assert.deepEqual(names(f),["alpha.txt","zeta.txt","folder"]);f.sorts[1].click();assert.deepEqual(names(f),["zeta.txt","alpha.txt","folder"]);
  f.sorts[3].click();assert.deepEqual(names(f),["folder","alpha.txt","zeta.txt"]);f.sorts[3].click();assert.deepEqual(names(f),["alpha.txt","folder","zeta.txt"]);assert.equal(rows(f)[2].children[3].textContent,"—");
  f.sorts[2].click();assert.deepEqual(names(f),["alpha.txt","folder","zeta.txt"]);
});
test("single click inspects; double click opens folder; breadcrumbs and back/forward navigate",async()=>{
  const f=fixture();await browse(f);const folder=rows(f).find(row=>content(row).includes("folder"));const before=f.requests.length;folder.click();assert.equal(f.requests.length,before);assert.match(content(f.nodes.preview),/\/etc\/folder/);folder.dispatch("dblclick");await settle();assert.equal(JSON.parse(f.requests.filter(x=>x.url==="/api/browse").at(-1).options.body).path,"/etc/folder");
  f.nodes["go-back"].click();await settle();assert.equal(JSON.parse(f.requests.filter(x=>x.url==="/api/browse").at(-1).options.body).path,"/etc");f.nodes["go-forward"].click();await settle();assert.equal(JSON.parse(f.requests.filter(x=>x.url==="/api/browse").at(-1).options.body).path,"/etc/folder");
  f.nodes.breadcrumbs.children.find(x=>x.textContent==="etc").click();await settle();assert.equal(JSON.parse(f.requests.filter(x=>x.url==="/api/browse").at(-1).options.body).path,"/etc");
});
test("bookmarks preserve host and path after reload",async()=>{
  const f=fixture();await browse(f);f.nodes["bookmark-current"].click();assert.deepEqual(JSON.parse(f.storage.get("grepmesh.bookmarks")),[{host:"server-100",path:"/etc"}]);const reload=fixture({storage:f.storage});await settle();reload.nodes["bookmarks-sidebar"].children[0].children[0].click();await settle();assert.deepEqual(JSON.parse(reload.requests.find(x=>x.url==="/api/browse").options.body),{host:"server-100",path:"/etc"});
});
test("host index and saved per-host timing are real API values",async()=>{const f=fixture();await browse(f);assert.match(content(f.nodes["host-sidebar"]),/Ready · 32 KB index/);assert.match(content(f.nodes["host-detail"]),/32 KB/);assert.match(content(f.nodes["host-detail"]),/12 ms/);f.nodes["show-telemetry"].click();await settle();assert.match(content(f.nodes["telemetry-records"]),/needle/);assert.match(content(f.nodes["telemetry-records"]),/15 ms/);assert.equal(f.nodes["file-table"].hidden,true);});
test("offline hosts stay visible, unknown index metrics are not invented",async()=>{const f=fixture({catalog:{hosts:[{id:"server-44",health:"offline",last_error:"Device timed out",roots:[]}]}});await settle();assert.match(content(f.nodes["host-sidebar"]),/server-44.*offline.*Device timed out/);assert.doesNotMatch(content(f.nodes["host-sidebar"]),/32 KB/);});
test("offline host with no roots never shows another host's locations",async()=>{const f=fixture({catalog:{hosts:[{id:"server-100",health:"healthy",roots:["/etc"]},{id:"server-44",health:"offline",roots:[]}]}});await settle();assert.match(content(f.nodes["locations-sidebar"]),/etc/);f.nodes["host-sidebar"].children.find(node=>content(node).includes("server-44")).click();assert.match(content(f.nodes["locations-sidebar"]),/No configured Locations/);assert.doesNotMatch(content(f.nodes["locations-sidebar"]),/etc|server-100/);});
test("selecting a device shows its configured folders in the center table",async()=>{
  const f=fixture({catalog:{hosts:[
    {id:"mac-m1",label:"Mac Mini M1",health:"healthy",roots:[{path:"/Users/user/Desktop",name:"Desktop"},{path:"/Users/user/Documents",name:"Documents"},{path:"/Users/user/Downloads",name:"Downloads"}]},
    {id:"server-100",label:"server-100",health:"healthy",roots:[{path:"/etc",name:"etc"}]}
  ]}});
  await settle();
  f.nodes["host-sidebar"].children.find(node=>content(node).includes("Mac Mini M1")).click();
  assert.match(content(f.nodes.results),/Desktop/);
  assert.deepEqual(names(f),["Desktop","Documents","Downloads"]);
  assert.ok(rows(f).every(row=>content(row).includes("Folder")));
  assert.equal(f.nodes["results-count"].textContent,"3 folders");
  assert.equal(f.requests.filter(request=>request.url==="/api/browse").length,0);
  const docs=rows(f).find(row=>content(row).includes("Documents"));docs.dispatch("dblclick");await settle();
  assert.deepEqual(JSON.parse(f.requests.filter(request=>request.url==="/api/browse").at(-1).options.body),{host:"mac-m1",path:"/Users/user/Documents"});
});
test("search narrows to the current folder using the Rust SearchArgs contract",async()=>{const f=fixture();await browse(f);f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();assert.deepEqual(JSON.parse(f.requests.find(x=>x.url==="/api/search").options.body),{query:"needle",hosts:"server-100",roots:["/etc"]});assert.equal(f.requests.filter(x=>x.url==="/api/telemetry").length,2);});
test("late search response cannot replace a newer directory view",async()=>{let finish;const f=fixture({responses:{"/api/search":()=>new Promise(resolve=>finish=resolve)}});await browse(f);f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();f.nodes.breadcrumbs.children.find(x=>x.textContent==="etc").click();await settle();finish({state:"complete",results:[]});await settle();assert.deepEqual(names(f),["alpha.txt","folder","zeta.txt"]);});
test("changing location clears stale search rows while the directory request is pending",async()=>{let finishBrowse;const f=fixture({responses:{"/api/search":{state:"complete",results:[{path:"/etc/needle.txt",preview_id:"needle",host_id:"server-100",line_number:1,snippet:"needle"}]},"/api/browse":()=>new Promise(resolve=>finishBrowse=resolve)}});await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();assert.deepEqual(names(f),["needle.txt"]);const root=f.nodes["locations-sidebar"].children[0];f.nodes["locations-sidebar"].dispatch("click",root.children[1]);await settle();assert.deepEqual(names(f),[]);assert.match(content(f.nodes.results),/Loading folder/);finishBrowse({entries});await settle();assert.deepEqual(names(f),["alpha.txt","folder","zeta.txt"]);});
test("empty views have visible instructions, not text hidden in class names",async()=>{const f=fixture();await settle();assert.match(content(f.nodes.results),/Select a Location to browse/);await browse(f);assert.match(content(f.nodes.preview),/No file selected/);assert.match(content(f.nodes.preview),/Choose a file or folder/);});
test("full breadcrumb remains visible but ancestors outside indexed roots cannot navigate",async()=>{const f=fixture({catalog:{hosts:[{id:"server-100",roots:["/Users/operator/projects"]}]}});await browse(f);const crumbs=f.nodes.breadcrumbs.children;assert.match(content(f.nodes.breadcrumbs),/Users.*operator.*projects/);for(const name of ["/","Users","operator"]){const crumb=crumbs.find(node=>node.textContent===name);assert.equal(crumb.disabled,true);assert.match(crumb.title,/outside indexed locations/);const before=f.requests.length;crumb.click();assert.equal(f.requests.length,before);}const root=crumbs.find(node=>node.textContent==="projects");assert.equal(root.disabled,false);root.click();await settle();assert.equal(JSON.parse(f.requests.filter(x=>x.url==="/api/browse").at(-1).options.body).path,"/Users/operator/projects");});
test("pending search shows progress and keeps host errors in a collapsed panel below the results",async()=>{const f=fixture({responses:{"/api/search":{job_id:"j",state:"running",partial:true,pending_hosts:["server-100"],host_status:[{host_id:"server-44",ok:false,error:"Timed out"}]},"/api/search/status":{state:"complete",results:[]}}});await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();assert.match(f.nodes["search-state"].className,/running/);assert.match(content(f.nodes["search-state"]),/ожидает узлов: 1/);assert.doesNotMatch(content(f.nodes["search-state"]),/unavailable/);assert.match(content(f.nodes["host-failures"]),/Timed out/);assert.equal(f.nodes["host-failures"].open,false);assert.match(content(f.nodes["host-failures"].children[0]),/Mesh errors · 1/);assert.match(content(f.nodes.results),/Идёт поиск/);assert.doesNotMatch(content(f.nodes.results),/No matching files/);await new Promise(resolve=>setTimeout(resolve,120));assert.match(content(f.nodes.results),/No matching files/);});
test("many host errors are summarized below the file table and scroll inside the disclosure",async()=>{
  const failures=Array.from({length:18},(_,index)=>({host_id:`server-${index}`,ok:false,error:`Connection timed out ${index}`}));
  const f=fixture({responses:{"/api/search":{state:"complete",results:[{path:"/docs/result.txt",preview_id:"result",host_id:"server-100",line_number:3,snippet:"needle"}],partial:true,host_status:failures}}});
  await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();
  const panel=f.nodes["host-failures"],html=fs.readFileSync("web/index.html","utf8"),css=fs.readFileSync("web/console.css","utf8");
  assert.equal(panel.hidden,false);assert.equal(panel.open,false);assert.match(content(panel.children[0]),/Mesh errors · 18/);assert.equal(panel.children[1].children.length,18);assert.match(content(f.nodes.results),/result.txt/);
  assert.ok(html.indexOf('id="host-failures"')>html.indexOf('id="file-table"'));
  assert.match(css,/\.host-failures-list[^}]*max-height:[^;}]+;[^}]*overflow:auto/s);
});
test("search metadata drives sorting and inspector alongside bounded preview",async()=>{const f=fixture({responses:{"/api/search":{state:"complete",results:[{path:"/etc/b.txt",preview_id:"b",host_id:"server-100",size:100,modified_ms:3000,created_ms:2000},{path:"/etc/a.txt",preview_id:"a",host_id:"server-100",size:200,modified_ms:1000,created_ms:null}]},"/api/preview":{text:"Preview text"}}});await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();f.sorts[1].click();assert.deepEqual(names(f),["b.txt","a.txt"]);assert.equal(rows(f)[0].children[1].textContent,"100 B");assert.notEqual(rows(f)[0].children[3].textContent,"—");assert.equal(rows(f)[1].children[3].textContent,"—");rows(f)[0].click();await settle();assert.match(content(f.nodes.preview),/100 B/);assert.match(content(f.nodes.preview),/Preview text/);f.sorts[2].click();assert.deepEqual(names(f),["a.txt","b.txt"]);});
test("binary files without an indexed OCR match explain why text preview is unavailable",async()=>{
  const f=fixture({responses:{"/api/browse":{entries:[{name:"scan.pdf",path:"/docs/scan.pdf",kind:"file",preview_id:"pdf"}]},"/api/preview":{text:"should not be requested"}}});
  await browse(f);rows(f)[0].click();await settle();
  assert.match(content(f.nodes.preview),/binary file/i);assert.match(content(f.nodes.preview),/search has indexed OCR text/i);
  assert.equal(f.requests.filter(request=>request.url==="/api/preview").length,0);
});
test("files beyond the preview-handle capacity show a clear explanation",async()=>{
  const f=fixture({responses:{"/api/browse":{entries:[{name:"overflow.txt",path:"/docs/overflow.txt",kind:"file",preview_unavailable:"handle_limit"}]}}});
  await browse(f);rows(f)[0].click();
  assert.match(content(f.nodes.preview),/Preview limit reached for this folder/);
  assert.equal(f.requests.filter(request=>request.url==="/api/preview").length,0);
});
test("selecting a match opens its line and previous/next walks every match in that file",async()=>{
  const previewText={first:"7 first match\n8 surrounding text",second:"19 second match\n20 surrounding text",third:"42 third match\n43 surrounding text"};
  const f=fixture({responses:{"/api/search":{state:"complete",results:[
    {path:"/docs/guide.txt",preview_id:"third",host_id:"server-100",line_number:42,snippet:"third match"},
    {path:"/docs/guide.txt",preview_id:"first",host_id:"server-100",line_number:7,snippet:"first match"},
    {path:"/docs/guide.txt",preview_id:"second",host_id:"server-100",line_number:19,snippet:"second match"},
    {path:"/docs/other.txt",preview_id:"other",host_id:"server-100",line_number:3,snippet:"other file"}
  ]},"/api/preview":options=>{const id=JSON.parse(options.body).preview_id;return{start_line:{first:7,second:19,third:42,other:3}[id],text:previewText[id]||"other file",...(id==="third"?{window_fallback:true}:{})};}}});
  await settle();f.nodes.query.value="match";f.nodes["search-form"].dispatch("submit");await settle();
  const second=rows(f).find(row=>row.title.includes("/docs/guide.txt:19"));second.click();await settle();
  assert.equal(JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body).view,"window");
  assert.equal(f.nodes["match-navigation"].hidden,false);assert.equal(f.nodes["match-position"].textContent,"Match 2 of 3 · line 19");
  let matchLine=descendants(f.nodes.preview).find(node=>node.className.includes("preview-match"));assert.equal(matchLine.dataset.line,"19");assert.match(content(matchLine),/second match/);assert.equal(descendants(f.nodes.preview).find(node=>node.className.includes("preview-code")).scrollTop,0);
  assert.equal(f.nodes["previous-match"].disabled,false);assert.equal(f.nodes["next-match"].disabled,false);
  f.nodes["previous-match"].click();await settle();assert.equal(JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body).preview_id,"first");assert.equal(f.nodes["match-position"].textContent,"Match 1 of 3 · line 7");assert.equal(f.nodes["previous-match"].disabled,true);
  f.nodes["next-match"].click();await settle();assert.equal(JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body).preview_id,"second");
  f.nodes["next-match"].click();await settle();assert.equal(JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body).preview_id,"third");assert.equal(f.nodes["match-position"].textContent,"Match 3 of 3 · line 42");
  matchLine=descendants(f.nodes.preview).find(node=>node.className.includes("preview-match"));assert.equal(matchLine.dataset.line,"42");assert.match(content(matchLine),/third match/);assert.match(content(f.nodes.preview),/bounded search context instead/);assert.equal(f.nodes["next-match"].disabled,true);
  f.nodes["previous-match"].click();await settle();assert.equal(JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body).preview_id,"second");assert.equal(f.nodes["match-position"].textContent,"Match 2 of 3 · line 19");assert.equal(f.nodes["previous-match"].disabled,false);
  rows(f).find(row=>row.title.includes("/docs/other.txt:3")).click();await settle();assert.equal(f.nodes["match-position"].textContent,"Match 1 of 1 · line 3");assert.equal(f.nodes["previous-match"].disabled,true);assert.equal(f.nodes["next-match"].disabled,true);
});
test("browse selection previews Markdown, JSON and HTML with source mode and bounded safe rendering",async()=>{
  const previewFiles=[
    {name:"guide.md",path:"/docs/guide.md",kind:"file",preview_id:"md"},
    {name:"settings.json",path:"/docs/settings.json",kind:"file",preview_id:"json"},
    {name:"page.html",path:"/docs/page.html",kind:"file",preview_id:"html"},
    {name:"large.json",path:"/docs/large.json",kind:"file",preview_id:"large"}
  ];
  const bodies={md:{start_line:1,text:"# Guide\n\nA formatted paragraph."},json:{start_line:1,text:'{"server":{"port":9419},"enabled":true,"items":[1,2]}'},html:{start_line:1,text:'<h1>Page</h1><img src="https://example.invalid/pixel"><script>parent.alert(1)</script>'},large:{start_line:1,text:'{"partial":',truncated:true}};
  const f=fixture({responses:{"/api/browse":{entries:previewFiles},"/api/preview":options=>bodies[JSON.parse(options.body).preview_id]}});
  await browse(f);
  const markdown=rows(f).find(row=>content(row).includes("guide.md"));markdown.click();await settle();
  const metadata=descendants(f.nodes.preview).find(node=>node.className.split(" ").includes("detail-panel"));assert.ok(metadata);assert.equal(metadata.open,false);assert.ok(descendants(f.nodes.preview).some(node=>node.textContent==="↗ Open in editor"));assert.ok(fs.readFileSync("web/index.html","utf8").includes('id="file-editor-dialog"'));assert.match(fs.readFileSync("web/console.css","utf8"),/height:min\(720px,76vh\)/);assert.doesNotMatch(fs.readFileSync("web/console.css","utf8"),/height:92vh/);
  let frame=descendants(f.nodes.preview).find(node=>node.tagName==="IFRAME");assert.ok(frame);assert.match(frame.srcdoc,/<h1>Guide/);assert.match(frame.srcdoc,/default-src 'none'/);assert.equal(frame.attributes.sandbox,"");assert.equal(frame.attributes.referrerpolicy,"no-referrer");
  assert.equal(f.requests.filter(request=>request.url==="/api/preview").length,1);assert.equal(JSON.parse(f.requests.find(request=>request.url==="/api/preview").options.body).view,"document");assert.equal(f.nodes["match-navigation"].hidden,true);
  const tabs=()=>descendants(f.nodes.preview).find(node=>node.className.includes("preview-tabs"));tabs().children.find(button=>button.textContent==="Source").click();assert.match(content(f.nodes.preview),/# Guide/);assert.ok(descendants(f.nodes.preview).some(node=>node.className.includes("preview-code")));
  const json=rows(f).find(row=>content(row).includes("settings.json"));json.click();await settle();assert.match(content(f.nodes.preview),/Object · 3 keys/);assert.match(content(f.nodes.preview),/port/);assert.match(content(f.nodes.preview),/9419/);
  const tree=descendants(f.nodes.preview).find(node=>node.className.split(" ").includes("json-tree")),details=tree.querySelectorAll("details");assert.equal(tree.attributes["aria-label"],"Formatted JSON preview");assert.ok(details.length>=3);assert.equal(details[0].open,true);assert.equal(details[1].open,false);
  descendants(f.nodes.preview).find(node=>node.textContent==="Expand all").click();assert.ok(details.every(node=>node.open));descendants(f.nodes.preview).find(node=>node.textContent==="Collapse all").click();assert.ok(details.every(node=>!node.open));
  const html=rows(f).find(row=>content(row).includes("page.html"));html.click();await settle();frame=descendants(f.nodes.preview).find(node=>node.tagName==="IFRAME");assert.match(frame.srcdoc,/<h1>Page/);assert.match(frame.srcdoc,/https:\/\/example\.invalid\/pixel/);assert.match(frame.srcdoc,/connect-src 'none'/);assert.equal(frame.attributes.sandbox,"");
  const large=rows(f).find(row=>content(row).includes("large.json"));large.click();await settle();assert.match(content(f.nodes.preview),/JSON formatting needs the complete file/);assert.match(content(f.nodes.preview),/bounded source/);assert.equal(tabs().children.find(button=>button.textContent==="Rendered").disabled,true);assert.equal(tabs().children.find(button=>button.textContent==="Source").disabled,false);
});
test("search match opens at its line and rendered Markdown loads the document from line one",async()=>{
  const f=fixture({responses:{"/api/search":{state:"complete",results:[{path:"/docs/guide.md",preview_id:"guide",host_id:"server-100",line_number:43,snippet:"needle"}]},"/api/preview":options=>JSON.parse(options.body).view==="document"?{start_line:1,text:"# Guide\n\nSection\n\nneedle appears here"}:{start_line:43,lines:[{line_number:43,text:"43 needle"}],text:"43 needle"}}});
  await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();rows(f)[0].click();await settle();
  assert.match(content(f.nodes.preview),/match at line 43/);assert.match(content(f.nodes.preview),/43.*needle/);assert.equal(descendants(f.nodes.preview).find(node=>node.className.includes("preview-match")).dataset.line,"43");
  const sourceRequest=JSON.parse(f.requests.filter(request=>request.url==="/api/preview").at(-1).options.body);assert.equal(sourceRequest.view,"window");
  const rendered=descendants(f.nodes.preview).find(node=>node.className.includes("preview-tabs")).children.find(button=>button.textContent==="Rendered");rendered.click();await settle();
  const requests=f.requests.filter(request=>request.url==="/api/preview");assert.equal(JSON.parse(requests.at(-1).options.body).view,"document");
  const frame=descendants(f.nodes.preview).find(node=>node.tagName==="IFRAME");assert.match(frame.srcdoc,/<h1>Guide/);assert.match(content(f.nodes.preview),/Source opens at the selected match/);
});
test("oversized document rendering falls back to the bounded match context",async()=>{
  const f=fixture({responses:{"/api/search":{state:"complete",results:[{path:"/docs/guide.md",preview_id:"guide",host_id:"server-100",line_number:43,snippet:"needle"}]},"/api/preview":options=>JSON.parse(options.body).view==="document"?{start_line:41,lines:[{line_number:41,text:"41 before"},{line_number:43,text:"43 needle"}],text:"41 before\n43 needle",document_fallback:true}:{start_line:41,lines:[{line_number:41,text:"41 before"},{line_number:43,text:"43 needle"}],text:"41 before\n43 needle"}}});
  await settle();f.nodes.query.value="needle";f.nodes["search-form"].dispatch("submit");await settle();rows(f)[0].click();await settle();
  descendants(f.nodes.preview).find(node=>node.className.includes("preview-tabs")).children.find(button=>button.textContent==="Rendered").click();await settle();
  assert.match(content(f.nodes.preview),/full document hit a safe read limit/);assert.match(content(f.nodes.preview),/43.*needle/);assert.ok(descendants(f.nodes.preview).some(node=>node.className.includes("preview-code")));assert.equal(descendants(f.nodes.preview).some(node=>node.tagName==="IFRAME"),false);
});
test("status polling recovers offline host health and locations without replacing active files",async()=>{let status={nodes:[],host_status:[{host_id:"server-100",ok:false,error:"Offline"}]};const f=fixture({catalog:{hosts:[{id:"server-100",health:"offline",roots:[]}]},responses:{"/api/host-status":()=>status}});await settle();assert.match(content(f.nodes["host-sidebar"]),/offline/);status={nodes:[{host_id:"server-100",roots:["/etc"],index_state:"Building",index_db_bytes:4096}],host_status:[{host_id:"server-100",ok:true}]};f.pollStatus();await settle();assert.match(content(f.nodes["host-sidebar"]),/healthy.*Building/);assert.doesNotMatch(content(f.nodes["host-sidebar"]),/offline/);assert.match(content(f.nodes["locations-sidebar"]),/etc/);await browse(f);const before=names(f);f.pollStatus();await settle();assert.deepEqual(names(f),before);status={nodes:status.nodes,host_status:[{host_id:"server-100",ok:false,error:"Timeout"}]};f.pollStatus();await settle();assert.match(content(f.nodes["host-sidebar"]),/offline/);assert.doesNotMatch(content(f.nodes["host-sidebar"]),/Building|4 KB/);});
test("older recovered node without roots requests one catalog refresh",async()=>{let catalogs=0;const f=fixture({responses:{"/api/catalog":()=>{catalogs++;return{hosts:[{id:"server-100",health:"offline",roots:catalogs>1 ? ["/etc"] : []}]};},"/api/host-status":{nodes:[{host_id:"server-100",index_state:"Ready"}],host_status:[{host_id:"server-100",ok:true}]}}});await settle();await settle();assert.equal(catalogs,2);assert.match(content(f.nodes["locations-sidebar"]),/etc/);assert.match(content(f.nodes["host-sidebar"]),/healthy/);f.pollStatus();await settle();assert.equal(catalogs,2);});
test("bookmark opened before catalog enables allowed breadcrumbs when roots arrive",async()=>{let finishCatalog;const storage=new Map([["grepmesh.bookmarks",JSON.stringify([{host:"server-100",path:"/Users/operator/Documents/work"}])]]);const f=fixture({storage,responses:{"/api/catalog":()=>new Promise(resolve=>finishCatalog=resolve),"/api/host-status":{nodes:[]}}});await settle();f.nodes["bookmarks-sidebar"].children[0].children[0].click();await settle();assert.equal(f.nodes.breadcrumbs.children.find(node=>node.textContent==="Documents").disabled,true);const before=names(f);finishCatalog({hosts:[{id:"server-100",roots:["/Users/operator/Documents"]}]});await settle();assert.equal(f.nodes.breadcrumbs.children.find(node=>node.textContent==="Documents").disabled,false);assert.equal(f.nodes["browser-heading"].textContent,"work");assert.deepEqual(names(f),before);f.nodes.breadcrumbs.children.find(node=>node.textContent==="Documents").click();await settle();assert.equal(JSON.parse(f.requests.filter(item=>item.url==="/api/browse").at(-1).options.body).path,"/Users/operator/Documents");});
test("status root hydration refreshes breadcrumb permissions",async()=>{let status={nodes:[],host_status:[{host_id:"server-100",ok:false}]};const storage=new Map([["grepmesh.bookmarks",JSON.stringify([{host:"server-100",path:"/etc/folder"}])]]);const f=fixture({storage,catalog:{hosts:[{id:"server-100",health:"offline",roots:[]}]},responses:{"/api/host-status":()=>status}});await settle();f.nodes["bookmarks-sidebar"].children[0].children[0].click();await settle();assert.equal(f.nodes.breadcrumbs.children.find(node=>node.textContent==="etc").disabled,true);status={nodes:[{host_id:"server-100",roots:["/etc"],index_state:"Ready"}],host_status:[{host_id:"server-100",ok:true}]};f.pollStatus();await settle();assert.equal(f.nodes.breadcrumbs.children.find(node=>node.textContent==="etc").disabled,false);assert.equal(f.nodes["browser-heading"].textContent,"folder");});
