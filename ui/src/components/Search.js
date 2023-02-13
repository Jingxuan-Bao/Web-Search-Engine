/**
 * Search page gets results from the server
 * Show results if there are any results returned
 * Otherwise show an image saying there aren't any results
 */
import React, { useState, useEffect } from "react";
import { FaSistrix } from "react-icons/fa";
import axios from "axios";
import Results from "./Results";
import Spinner from "./Spinner";

const Search = (props) => {
  const [state, setState] = useState(
    props.location.state ? props.location.state : ""
  );
  const [query, setQuery] = useState(window.location.href.split("/")[4]);
  const [result, setResult] = useState([]);
  const [time, setTime] = useState(0);
  const [loading, setLoading] = useState(false);

  // go back to home page
  const homepage = () => {
    props.history.push("/");
  };

  // handle pressing key "enter"
  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      this.searchStrugle();
    }
  };

  // use axios to fetch results from server
  const searchStrugle = async (e) => {
    e.preventDefault();
    setQuery(state);
    setLoading(true);
    let startTime = new Date().getTime();
    try {
      await axios
        .get(`http://0.0.0.0:8000/search?query=${state}`)
        .then((res) => {
          if (res.status === 200) {
            setResult(res.data);
            setTime(new Date().getTime() - startTime);
          } else {
            setResult([]);
            console.log("no result!!!!!");
          }
          setLoading(false);
        });
    } catch (error) {
      console.log(error);
      setLoading(false);
    }
  };

  // use useEffect to fetch again when user tries to search again
  useEffect(() => {
    async function getResults() {
      setQuery(state);
      setLoading(true);
      let startTime = new Date().getTime();
      if (props.location.state) {
        try {
          await axios
            .get(`http://0.0.0.0:8000/search?query=${state}`)
            .then((res) => {
              if (res.status === 200) {
                setResult(res.data);
                setTime(new Date().getTime() - startTime);
              } else {
                setResult([]);
                console.log("no result!!!!!");
              }
              setLoading(false);
            });
        } catch (error) {
          console.log(error);
          setLoading(false);
        }
      }
    }
    getResults();
  }, []);

  return (
    <div className="search">
      <div className="search-form">
        <div className="search-form-logo">
          <img
            src="/logo.png"
            alt="tinylogo"
            width="150"
            height="100"
            onClick={homepage}
          />
        </div>
        <div className="search-form-input">
          <form className="home-form" onSubmit={searchStrugle}>
            <input
              type="text"
              className="home-input"
              value={state}
              onChange={(e) => setState(e.target.value)}
            />
            <FaSistrix
              className="search-icon"
              onMouseDown={searchStrugle}
              onKeyPress={handleKeyPress}
            />
          </form>
        </div>
      </div>
      {loading ? (
        <Spinner />
      ) : (
        <div>
          {result === null || result === undefined || result.length === 0 ? (
            <div>
              <div style={{ marginTop: "40px", marginLeft: "40px" }}>
                <p>
                  Your search - <strong>{query}</strong> - did not match any
                  documents.
                </p>
                <div style={{ marginTop: "20px" }}>
                  <p>Suggestions:</p>
                  <ul style={{ marginTop: "5px", marginLeft: "30px" }}>
                    <li>Make sure all words are spelled correctly.</li>
                    <li>Try different keywords.</li>
                    <li>Try more general keywords.</li>
                  </ul>
                </div>
              </div>
              <img
                src="https://israelguidedog.org.il/en/wp-content/themes/igd/images/noresult.png"
                alt=""
                style={{
                  marginTop: "70px",
                  marginLeft: "50px",
                  width: "60%",
                }}
              ></img>
            </div>
          ) : (
            <div>
              <Results result={result} time={time} />
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default Search;
