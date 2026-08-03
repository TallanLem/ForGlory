(() => {
  "use strict";

  const STATE_KEY = "__forglory_query_state_v1";
  const state = history.state && typeof history.state === "object"
    ? history.state
    : {};
  const saved = state[STATE_KEY];
  const navigation = performance.getEntriesByType?.("navigation")?.[0];
  const navigationType = navigation?.type || "navigate";
  const canRestore = navigationType === "reload" || navigationType === "back_forward";

  // A clean URL is intentionally shown in the address bar. On reload or a
  // non-bfcache history navigation, briefly restore the hidden query before
  // the body is rendered so Flask receives the same filters again.
  if (
    !window.location.search &&
    canRestore &&
    saved &&
    saved.path === window.location.pathname &&
    typeof saved.query === "string" &&
    saved.query.startsWith("?")
  ) {
    window.location.replace(
      `${window.location.pathname}${saved.query}${window.location.hash}`
    );
    return;
  }

  // Keep the complete filter state in this history entry, then remove only
  // the visible query string. The rendered page and all internal GET forms
  // continue to work with their original parameters.
  if (window.location.search) {
    const nextState = {
      ...state,
      [STATE_KEY]: {
        path: window.location.pathname,
        query: window.location.search,
      },
    };
    history.replaceState(
      nextState,
      document.title,
      `${window.location.pathname}${window.location.hash}`
    );
  }
})();
