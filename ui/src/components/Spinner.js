/**
 * Show a spinner.gif when we are still waiting for the
 * search results to be returned by the server to
 * indicate to the user that the page is loading
 */
import React from "react";

const Spinner = () => (
  <div>
    <img
      src={"/spinner.gif"}
      alt="Loading..."
      style={{ display: "block", width: "200px", margin: "auto" }}
    />
  </div>
);

export default Spinner;
