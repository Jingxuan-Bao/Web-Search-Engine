/**
 * The home page shows the app logo and a search bar,
 * user clicks button or presses enter to enable searching
 */
import React, { useState } from "react";
import { FaSistrix } from "react-icons/fa";

const Home = (props) => {
  const [state, setstate] = useState(""); // represents the search terms

  // go to the search page
  const searchStrugle = () => {
    if (state !== "") {
      props.history.push({ pathname: `/search`, state });
    }
  };

  // handle pressing key "enter"
  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      searchStrugle();
    }
  };

  return (
    <div className="home">
      <div className="home-container">
        <div className="home-logo">
          <img src="/logo.png" alt="logo" width="350" height="250" />
        </div>
        <form className="home-form" onSubmit={searchStrugle}>
          <input
            type="text"
            className="home-input"
            onChange={(e) => setstate(e.target.value)}
            value={state}
          />
          <div className="home-btn-container">
            <input className="home-btn" type="submit" value="Strugle Search" />
          </div>
          <FaSistrix
            className="search-icon"
            onMouseDown={searchStrugle}
            onKeyPress={handleKeyPress}
          />
        </form>
      </div>
    </div>
  );
};

export default Home;
