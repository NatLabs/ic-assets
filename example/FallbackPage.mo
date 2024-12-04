module {
    public func get_fallback_page() : Text {
        // Chat GPT generated 404 page
        "<!DOCTYPE html>
            <html lang=\"en\">
            <head>
                <meta charset=\"UTF-8\">
                <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
                <meta http-equiv=\"X-UA-Compatible\" content=\"ie=edge\">
                <title>Fallback Page</title>
                <style>
                    body {
                        margin: 0;
                        padding: 0;
                        font-family: Arial, sans-serif;
                        background-color: white;
                        color: black;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        text-align: center;
                    }

                    .container {
                        max-width: 600px;
                        padding: 20px;
                        border: 1px solid black;
                    }

                    h1 {
                        font-size: 5em;
                        margin: 0;
                    }

                    h2 {
                        font-size: 2em;
                        margin-bottom: 20px;
                    }

                    p {
                        font-size: 1.2em;
                        margin: 10px 0;
                    }

                    .funny-message {
                        font-style: italic;
                        font-weight: bold;
                    }

                    .back-link {
                        text-decoration: none;
                        color: black;
                        border: 1px solid black;
                        padding: 10px 20px;
                        display: inline-block;
                        margin-top: 20px;
                    }

                    .back-link:hover {
                        background-color: black;
                        color: white;
                    }
                </style>
            </head>
            <body>
                <div class=\"container\">
                    <h1>Fallback Page</h1>
                    <h2>Oops! Page not found.</h2>
                    <p class=\"funny-message\" id=\"funnyMessage\"></p>
                    <a href=\"javascript:void(0);\" class=\"back-link\" onclick=\"window.location.href = window.location.origin + '/homepage' + window.location.search;\">Take me home!</a>
                </div>

                <script>
                    const messages = [
                        \"The page you're looking for has taken a vacation.\",
                        \"Looks like this page got lost in the matrix.\",
                        \"Oops! This is a digital black hole.\",
                        \"This page must be hiding in a parallel universe.\",
                        \"We broke it. Let’s pretend this never happened.\",
                        \"You’ve found the secret fallback page! (Not really.)\",
                    ];

                    function getRandomMessage() {
                        return messages[Math.floor(Math.random() * messages.length)];
                    }

                    cookieStore.get(\"__ic_asset_motoko_lib_error__\").then(cookie => {
                        let message = cookie ? cookie.value : getRandomMessage();
                        document.getElementById(\"funnyMessage\").textContent = message;
                    });

                </script>
            </body>
            </html>
        "
    }
}