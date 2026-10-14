"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

const source = fs.readFileSync(
  path.resolve(__dirname, "../../src/litoral_trace/static/js/us_lacey_source_inspector.js"),
  "utf8",
).split("/* Native <dialog>")[0];

function harness(fetchImpl) {
  let made = 0;
  const revoked = [];
  const scope = {
    fetch:fetchImpl,
    AbortController,
    DOMException,
    URL:{
      createObjectURL:() => "blob:mock-" + ++made,
      revokeObjectURL:(url) => revoked.push(url),
    },
  };
  vm.createContext(scope);
  vm.runInContext(source + "\nthis.EvidenceSession=EvidenceSession;this.PdfEvidenceRenderer=PdfEvidenceRenderer;", scope);
  return {scope, revoked};
}
const response = {
  ok:true, headers:{get:() => "application/pdf"},
  blob: async () => ({type:"application/pdf"}),
};
const abortError = () => new DOMException("Aborted", "AbortError");

test("hover -> pin shares a pending authorized GET, then releases the temporary blob", async () => {
  let gets = 0;
  let resolveFetch;
  const {scope, revoked} = harness(() => {
    ++gets;
    return new Promise((resolve) => { resolveFetch = resolve; });
  });
  const session = new scope.EvidenceSession();
  const first = session.acquire({key:"source|revision-a",url:"/source/5"});
  const pinned = session.acquire({key:"source|revision-a",url:"/source/5"});
  assert.strictEqual(first, pinned);
  assert.equal(gets, 1);
  resolveFetch(response);
  const [a,b] = await Promise.all([first,pinned]);
  assert.strictEqual(a, b);
  assert.equal(a.objectUrl, "blob:mock-1");
  session.release();
  assert.deepEqual(revoked, ["blob:mock-1"]);
  assert.equal(session.active, null);
});

test("new source revision cancels an obsolete fetch and never reuses its bytes", async () => {
  const requests = [];
  const {scope, revoked} = harness((_url, options) => new Promise((resolve,reject) => {
    requests.push({resolve,reject, signal:options.signal});
    options.signal.addEventListener("abort", () => reject(abortError()), {once:true});
  }));
  const session = new scope.EvidenceSession();
  const old = session.acquire({key:"source|rev1",url:"/source/1"});
  const latest = session.acquire({key:"source|rev2",url:"/source/1"});
  assert.equal(requests.length,2);
  assert.equal(requests[0].signal.aborted,true);
  requests[1].resolve(response);
  await assert.rejects(old, (err) => err.name === "AbortError");
  assert.equal((await latest).key, "source|rev2");
  session.release();
  assert.deepEqual(revoked,["blob:mock-1"]);
});

function fakeRenderer() {
  const {scope}=harness(async () => response);
  const controls={
    page:{textContent:""},zoom:{textContent:""},
    previous:{disabled:true},next:{disabled:true},
  };
  let width=0, height=0;
  const canvas={
    getContext:() => ({}),
    get width(){return width;},set width(value){width=value;},
    get height(){return height;},set height(value){height=value;},
  };
  const renderer=new scope.PdfEvidenceRenderer(canvas, controls);
  const rendered=[];
  let cancelled=0;
  const pdf={
    numPages:4,
    destroy:async () => {},
    getPage:async (pageNo) => ({
      getViewport:({scale})=>({width:200*scale,height:300*scale}),
      cleanup:()=>{},
      render:() => {
        let rejectRender;
        const promise=new Promise((resolve,reject) => {
          rejectRender=reject;
          setTimeout(() => {rendered.push(pageNo);resolve();},15);
        });
        return {
          promise,
          cancel:()=> {++cancelled;rejectRender({name:"RenderingCancelledException"});},
        };
      },
    }),
  };
  renderer.pdf=pdf;
  return {renderer,controls,rendered,canvas,getCancelled:()=>cancelled};
}

test("rapid next-page and zoom requests serialize and settle on latest state", async () => {
  const x=fakeRenderer();
  const p1=x.renderer.renderPage();
  const p2=x.renderer.nextPage();
  const p3=x.renderer.nextPage();
  const p4=x.renderer.setZoom(1.4);
  await Promise.all([p1,p2,p3,p4]);
  assert.equal(x.controls.page.textContent,"Page 3 of 4");
  assert.equal(x.controls.zoom.textContent,"140%");
  assert.equal(x.canvas.width,280);
  x.renderer.destroy();
  assert.equal(x.canvas.width,0);
});

test("destroy invalidates a delayed getPage so a stale page cannot redraw", async () => {
  const x=fakeRenderer();
  let releasePage;
  x.renderer.pdf.getPage=()=>new Promise((resolve)=> {releasePage=resolve;});
  const result=x.renderer.renderPage();
  await Promise.resolve(); // allow getPage to begin
  x.renderer.destroy();
  releasePage({cleanup:()=>{},getViewport:()=>{throw Error("stale draw");}});
  await result;
  assert.equal(x.controls.page.textContent,"");
  assert.equal(x.canvas.width,0);
});
