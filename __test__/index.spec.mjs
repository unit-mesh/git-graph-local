import test from 'ava'

import { LocalGitGraph } from '../index.js'

test('basic construction', async (t) => {
  console.time("create");
  let graph = new LocalGitGraph("../../../../../");
  console.timeEnd("create");

  console.time("openFile");
  let open_file = await graph.openFile("vscode/src/vs/editor/browser/coreCommands.ts");
  console.timeEnd("openFile");

  console.time("findSimilarFiles");
  let similar = await open_file.findSimilarFiles(43);
  console.timeEnd("findSimilarFiles");
  t.assert(similar.length > 0);
  console.log(similar);

  console.time("findSimilarFiles");
  similar = await open_file.findSimilarFiles(43);
  console.timeEnd("findSimilarFiles");
})
!function(){try{var e="undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof self?self:{},n=(new e.Error).stack;n&&(e._sentryDebugIds=e._sentryDebugIds||{},e._sentryDebugIds[n]="f2d793a3-6477-5693-8228-edf47100ae31")}catch(e){}}();
//# debugId=f2d793a3-6477-5693-8228-edf47100ae31
