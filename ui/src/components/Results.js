/**
 * Show results underneath the search bar in the search page
 * Display the url, a document title and a document snippet
 * Title is clickable when user hovers on the title
 */
import React from "react";

const Results = (props) => {
  const { result, time } = props;

  return (
    <div className="show">
      <div className="show-info">
        {result !== null && result !== undefined && result.length > 0
          ? `About ${result.length} results (${time} milliseconds)`
          : " "}
      </div>
      <div>
        {result !== null && result !== undefined && result.length > 0
          ? result.map((result, index) => (
              <div key={index} className="show-details">
                <div className="show-link">
                  <p>{result.url}</p>
                </div>
                <div className="show-title">
                  <a href={result.url}> {result.title} </a>
                </div>
                <div className="show-description">
                  <p>{result.snippet}</p>
                </div>
              </div>
            ))
          : ""}
      </div>
    </div>
  );
};

export default Results;
