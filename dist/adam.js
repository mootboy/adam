export const $APP = {};
export const shadow$provide = {};
/*

 Copyright The Closure Library Authors.
 SPDX-License-Identifier: Apache-2.0
*/
export const start=function(b){b.registerCommand.call(b,"adam:status",{description:"Show adam status",handler:function(a,c){a=c.ui;return a.notify.call(a,"adam is running","info")}});return null};