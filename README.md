<div id="top"></div>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <!-- <a href="https://github.com/Ark667/DynamoDb2MongoDb">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a> -->

<h3 align="center">DynamoDb2MongoDb</h3>

  <p align="center">
    Simple .Net5 application for transfering DynamoDb table data to MongoDb collection.
    <br />
    <a href="https://github.com/Ark667/DynamoDb2MongoDb"><strong>Explore the docs »</strong></a>
    <br />    
    <a href="https://github.com/Ark667/DynamoDb2MongoDb/issues">Report Bug</a>
    ·
    <a href="https://github.com/Ark667/DynamoDb2MongoDb/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#usage">Usage</a></li>
    <!-- <li><a href="#roadmap">Roadmap</a></li> -->
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <!-- <li><a href="#acknowledgments">Acknowledgments</a></li> -->
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

<!-- [![Product Name Screen Shot][product-screenshot]](https://example.com) -->

This project was intended to make a fast data transfer from a bunch of DynamoDb tables to MongoDb collections. 
Provisioned DynamoDb tables became too expensive and decided to migrate to managed MongoDb service on K8s cluster.
MongoDb also provide some desirable querying features not present on DynamoDb.

<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [.Net5](https://dotnet.microsoft.com/download/dotnet/5.0)
* [AWSSDK.DynamoDBv2](https://github.com/aws/aws-sdk-net/)
* [MongoDb.Driver](https://docs.mongodb.com/drivers/csharp/)
* [CommandLineParser](https://github.com/commandlineparser/commandline)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

You can execute current release with Docker.

```sh
docker run --rm --network=host ghcr.io/ark667/dynamodb2mongodb:master copy 
    --dynamoaccesskey *** 
    --dynamosecretaccesskey *** 
    --dynamoregion [region] 
    --mongoconnectionstring "mongodb://[host]:[port]/[database]" 
    --dynamotable [tablename]
```

You can also clone the repo and build it yourself.

1. Clone the repo

   ```sh
   git clone https://github.com/Ark667/DynamoDb2MongoDb.git
   ```

2. Build application

   ```sh
   dotnet build DynamoDb2MongoDb.sln
   ```

3. Execute help option

   ```sh
   .\DynamoDb2MongoDb.exe --help
   ```

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

Basic usage is pretty straightforrward. Just call copy verb with required keys and parameters.

```sh
.\DynamoDb2MongoDb.exe copy
    --dynamoaccesskey *** 
    --dynamosecretaccesskey *** 
    --dynamoregion [region] 
    --mongoconnectionstring "mongodb://[host]:[port]/[database]" 
    --dynamotable [tablename]
```

Can also be executed from Docker container.

```sh
docker build -f ".\DynamoDb2MongoDb\Dockerfile" .
```

```sh
docker run --rm --network=host dynamodb2mongodb copy 
    --dynamoaccesskey *** 
    --dynamosecretaccesskey *** 
    --dynamoregion [region] 
    --mongoconnectionstring "mongodb://[host]:[port]/[database]" 
    --dynamotable [tablename]
```



<p align="right">(<a href="#top">back to top</a>)</p>


<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Aingeru Medrano - [@AingeruBlack](https://twitter.com/AingeruBlack) <!-- - email@email_client.com -->

Project Link: [https://github.com/Ark667/DynamoDb2MongoDb](https://github.com/Ark667/DynamoDb2MongoDb)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS
## Acknowledgments

* []()
* []()
* []()

<p align="right">(<a href="#top">back to top</a>)</p>
 -->



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/Ark667/DynamoDb2MongoDb.svg?style=for-the-badge
[contributors-url]: https://github.com/Ark667/DynamoDb2MongoDb/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Ark667/DynamoDb2MongoDb.svg?style=for-the-badge
[forks-url]: https://github.com/Ark667/DynamoDb2MongoDb/network/members
[stars-shield]: https://img.shields.io/github/stars/Ark667/DynamoDb2MongoDb.svg?style=for-the-badge
[stars-url]: https://github.com/Ark667/DynamoDb2MongoDb/stargazers
[issues-shield]: https://img.shields.io/github/issues/Ark667/DynamoDb2MongoDb.svg?style=for-the-badge
[issues-url]: https://github.com/Ark667/DynamoDb2MongoDb/issues
[license-shield]: https://img.shields.io/github/license/Ark667/DynamoDb2MongoDb.svg?style=for-the-badge
[license-url]: https://github.com/Ark667/DynamoDb2MongoDb/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/aingeru/
[product-screenshot]: images/screenshot.png
