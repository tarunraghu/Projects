-- Drop and create database 
DROP DATABASE IF EXISTS equinox;
CREATE DATABASE equinox;

-- Use database created 
USE equinox;

-- Table 1 User_t contains end user information for the learning platform
DROP TABLE IF EXISTS User_t;
CREATE TABLE User_t (
    UserID INT PRIMARY KEY AUTO_INCREMENT,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    EmailID VARCHAR(100) UNIQUE NOT NULL,
    PhoneNumber VARCHAR(15),
    Address VARCHAR(255),
    Gender VARCHAR(10),
    Password VARCHAR(255) NOT NULL
)  AUTO_INCREMENT=1001;

INSERT INTO User_t (FirstName, LastName, EmailID, PhoneNumber, Address, Gender, Password) VALUES
('Izaiah', 'Tromp', 'izaiah.tromp@example.com', '(914)998-9405x3', '72700 Viola Lock Apt. 244 South Gino, CT 00386', 'Male', '924eb91074bf8793ddafa01c868a77dc01471aea'),
('Bernie', 'Schneider', 'bernie.schneider@example.com', '417.801.1323', '581 Ferry Circles Apt. 528 Glovertown, PA 09347', 'Male', 'b357f5f5c60f18b615138e0c93f20e3ae275442b'),
('Elody', 'Welch', 'elody.welch@example.com', '(009)896-8917', '174 Koch Turnpike Onieberg, SD 76796-5139', 'Female', '7b4808b56bb9a3f020580ff9ad0af08d0ecbaa36'),
('Cortez', 'Osinski', 'cortez.osinski@example.com', '(756)477-4882x1', '621 Padberg Loaf West Amani, ND 37992-1356', 'Male', 'f3297a0ca1dca324f31368b1275422fb8bf335f5'),
('Ludwig', 'Kihn', 'ludwig.kihn@example.com', '(781)613-1903', '54683 Morar Ford Suite 185 North Rickstad, AZ 99851-5684', 'Male', '6ca6e9e876140389a34400d284307f6d6121720c'),
('Marietta', 'Heller', 'marietta.heller@example.com', '940.382.5510x85', '1127 Jeramy Roads Suite 006 West Estelfort, SD 33477-9095', 'Female', 'a9e1ccbc85c28d95ade1d10546371a93ce5bfa48'),
('Miracle', 'Kuphal', 'miracle.kuphal@example.com', '266-056-5404', '9123 Purdy Walk Apt. 142 Lake Nigeltown, ME 01682', 'Female', 'ac6b21c3f996bdff8bea0fccbfbfec26661bbb9c'),
('Enoch', 'Ullrich', 'enoch.ullrich@example.com', '1-578-519-9169', '9716 McDermott Spurs Apt. 948 Lake Sabrina, SC 70719', 'Male', '6c48b0fe3ae754dc469972c42cd8b21c86615011'),
('Howell', 'Gerhold', 'howell.gerhold@example.com', '845.921.9705', '826 Lindgren Heights South Abdielstad, LA 96532-6687', 'Male', 'b33047b7063489dcb608d26ba831a79d94b41cda'),
('Hanna', 'Upton', 'hanna.upton@example.com', '558.905.7779x05', '5434 Sanford Land North Pierrefort, ID 24947-9054', 'Female', '5fab4acc6aaeb21e0baf72cab2c62609a85ee455'),
('Mekhi', 'Renner', 'mekhi.renner@example.com', '7402245281', '993 Jacobi Greens Jacobsbury, KY 73077', 'Male', '463ebf679be7ae8015e55103e353b87ef2023b2c'),
('Manuela', 'Spinka', 'manuela.spinka@example.com', '1-906-288-1290x', '7938 Mateo Terrace Apt. 381 Fosterborough, HI 49132-9993', 'Female', '540cbf63caca8dada977dba9586325986af3b035'),
('Novella', 'Williamson', 'novella.williamson@example.com', '1038355851', '100 Zemlak Tunnel Apt. 523 Lake Odessachester, VA 56897', 'Female', '26f2de5a438ec56efb434bbbfa680fecda76466e'),
('Clare', 'Wolf', 'clare.wolf@example.com', '(868)799-1040', '52729 Lynch Tunnel Suite 457 North Kieranside, UT 63920-7986', 'Female', 'd1b4e9b4fc7888d35ade8919307b5c07de8d6534'),
('Else', 'Larkin', 'else.larkin@example.com', '355.593.3040x82', '6092 Shyanne Shores North Marianneview, OH 37624-1660', 'Female', '3ade8109f0bb63006b1d2d33776fc547282ef475'),
('Lavinia', 'Kulas', 'lavinia.kulas@example.com', '494.515.3673', '9862 Friesen Way Apt. 071 East Krishaven, NV 84985-4377', 'Female', '977deaae5fa4057e8f58a0a51a48ea8419864b9f'),
('Pablo', 'Morar', 'pablo.morar@example.com', '+27(4)753482219', '2874 Swaniawski Locks Funkborough, NV 00578-9831', 'Male', 'c08553062ee3f9503bd8681755764a9a959abe06'),
('Hollis', 'Corwin', 'hollis.corwin@example.com', '+98(3)412895579', '26332 Swaniawski Ridge Apt. 520 Bechtelarchester, DE 67943', 'Male', '9be5635664c7cb83bc38957b71f04644b1f474dc'),
('Queenie', 'Lueilwitz', 'queenie.lueilwitz@example.com', '597-523-8653x13', '81050 Gerhold Freeway West Russell, LA 83043', 'Female', 'c545f0c5a32da17c3d0e436b682a368e6063dd8d'),
('Doyle', 'Ledner', 'doyle.ledner@example.com', '201-546-7097x00', '3772 Dino Hills Suite 918 Markshaven, SC 75062-7582', 'Male', '4e38514ccfefcbbc8fb3b9502ac96c9a46ed3af1'),
('Penelope', 'Hilpert', 'penelope.hilpert@example.com', '(504)770-1163x2', '453 Ziemann Underpass New Lilla, CA 78991', 'Female', '550d2afd2abd35cf23562b73524d8eadf41e8df9'),
('Celestino', 'Keebler', 'celestino.keebler@example.com', '(421)426-1417x2', '120 Golda Pass Brownville, MA 58663-2584', 'Male', 'fc65bd651f93d034a42283eb9c7feb4470c70425'),
('Tanya', 'Howe', 'tanya.howe@example.com', '693-082-7964x67', '13935 Raynor Pass Lake Ariel, AK 56920', 'Female', 'cc824d5190fe97665a3d392b059772de4e3e2d4b'),
('Priscilla', 'Schaden', 'priscilla.schaden@example.com', '233.750.0808', '2103 Gutmann Ports Apt. 938 Donavonmouth, ME 08225', 'Female', 'e7b41768ac1f809f68e5058f8172da9ca1c34061'),
('Carlo', 'Emard', 'carlo.emard@example.com', '+09(9)423539390', '34199 Max Drive Garretberg, CT 92477-8398', 'Male', 'ced9cd2310f65e93b09dad488ff6eefba10c6ac9'),
('Sabryna', 'Franecki', 'sabryna.franecki@example.com', '1-463-776-5542x', '82300 Madisyn Vista New Mikaylaland, VT 21676-1183', 'Female', '3e4de83d4174b26b2ce5a3e460467214872b08e8'),
('Maia', 'Considine', 'maia.considine@example.com', '754-301-1291x68', '780 Hagenes Vista Apt. 918 South Caraside, VA 91319', 'Female', '2bf2ca110e184ecbc56cdcd1f4505fc63fd02d92'),
('Isobel', 'Schumm', 'isobel.schumm@example.com', '(264)034-8259x0', '796 Andy Flat Suite 731 Mervinfort, OK 25139', 'Female', '9cb2ab78db6d9d5832ee5908907992d5cebdaf5c'),
('Laurel', 'Schuster', 'laurel.schuster@example.com', '142.323.3627x94', '450 Dicki Highway Suite 385 Konopelskiville, NJ 65192', 'Female', '4ae004ddc627cb69b19a123ee8bb0ba854acd92c'),
('Linnea', 'Abernathy', 'linnea.abernathy@example.com', '1-718-316-5450', '862 Spencer Harbor West Levi, IN 00831-5796', 'Female', '3e642d982bd3af3e17971701eec2a42021782e4a'),
('Arthur', 'Funk', 'arthur.funk@example.com', '1-664-407-4939', '423 Rohan Valley Cristobalstad, NM 22015-8330', 'Male', 'e42cd5e383c966fab0b758625682ea05de7b393b'),
('Brett', 'Keebler', 'brett.keebler@example.com', '5912521660', '642 Casper Turnpike Apt. 178 Beattybury, SD 66329', 'Male', 'ebf69c9f7238be20040b49ab8934195ab9cb2d9b'),
('Audrey', 'Marquardt', 'audrey.marquardt@example.com', '657.678.8675x49', '741 Connelly Points East Anabelleport, MI 72827', 'Female', '720edb092820e0411de1d6fa708774d3faf1a474'),
('Cristina', 'Blanda', 'cristina.blanda@example.com', '1-135-351-5646', '0743 Jast Fields Suite 714 North Lexusmouth, MS 97799-8143', 'Female', '46b4c0eb988d337345128b157be8f58197a83694'),
('Serenity', 'Rowe', 'serenity.rowe@example.com', '971-332-3731', '006 Dickinson Shoals Suite 243 Breitenbergport, NH 60277', 'Female', 'e4a145e48b12510b8e51d6cf21e67cac21b7983e'),
('Ayla', 'Mayert', 'ayla.mayert@example.com', '1-114-489-5861x', '489 Nienow Corner Suite 083 Alenashire, SD 59267-7225', 'Female', '0e98df8053f79bdbb2a94ae09174efb95bfe7e7b'),
('Cathy', 'Hackett', 'cathy.hackett@example.com', '587.819.6508', '5309 Abshire Tunnel Sigridside, KS 07048-5971', 'Female', '3c088866d92db52721207e989ca9baf8e0cd4801'),
('Carson', 'D\'Amore', 'carson.d\'amore@example.com', '(815)350-2962x4', '330 Eunice Rest Port Favianberg, NE 21029', 'Male', '0f92bf8ab1c92cb1fb4370c78508d6861fc624e5'),
('Kimberly', 'Pacocha', 'kimberly.pacocha@example.com', '8359950505', '94641 Bednar Walks Suite 818 West Amelia, VA 80735-5293', 'Female', 'cdaf7ffa1c7e162b798317a2df0a2ce1c0909a38'),
('Beau', 'Maggio', 'beau.maggio@example.com', '(904)775-2895', '33517 Christian Avenue Apt. 909 North Velmaland, DC 02928', 'Male', 'af4e61ba6e1648c70f8b8cc9c0b5b60e50b3f604'),
('Eryn', 'Kuhic', 'eryn.kuhic@example.com', '1-490-188-2527', '57220 O\'Connell Hill Saigetown, AZ 48775', 'Female', '990d9dfb474c085902b6ca4035a6e5c057900cdc'),
('Gerson', 'O\'Kon', 'gerson.o\'kon@example.com', '844-426-0996x13', '20855 Swift Ford North Tyrelbury, IA 40059-3407', 'Male', '34e76c4313469d058a78dfcc458dab2dd5cb3ed9'),
('Grace', 'Lubowitz', 'grace.lubowitz@example.com', '+61(5)020818578', '184 Pollich Light Lake Kaleighfurt, CA 30157-6334', 'Female', '561c8ceb53c1651df9a2bdef399439006b1fad71'),
('Josh', 'Mayer', 'josh.mayer@example.com', '732-262-8639', '446 Pasquale Via East Gageberg, GA 65277', 'Male', '5ec7a8231cf8cac13fc1738f918e8ae53100e748'),
('Ethyl', 'Dietrich', 'ethyl.dietrich@example.com', '861-544-8387x48', '07111 Elda Ways Gutkowskiberg, WI 98897', 'Female', '0fd1a853b77ceb27be148f8f7a6aea00100d37ff'),
('Alek', 'Vandervort', 'alek.vandervort@example.com', '1-827-137-5093x', '706 Rossie Views East Caitlynville, NH 12288-0219', 'Male', '1f0541cce2a24db7ab60926a1b70896493442c1f'),
('Johnathon', 'Greenfelder', 'johnathon.greenfelder@example.com', '052.353.8433x03', '7480 Torey Vista West Myrl, VT 95971-2089', 'Male', 'dbf8a3bc6780a130e1dbee2033a208893903cba6'),
('Liza', 'Hayes', 'liza.hayes@example.com', '736-707-4236x54', '75657 Helga Oval Suite 372 Kshlerinville, AZ 05360-7676', 'Female', '28d3835463c7c252524d08c6d7c64c239bcf8cd7'),
('Brenda', 'Bode', 'brenda.bode@example.com', '379.956.2835', '51491 Stamm View Cliffordchester, MO 41823', 'Female', '821c0160694b3122dc1ebfe0e6d549ea618c2729'),
('Dale', 'Quitzon', 'dale.quitzon@example.com', '1-600-185-2167x', '772 Cummerata Forest Baumbachfurt, ID 51531', 'Male', 'cb1b4a42e19ffee3aa6d2a8d9a35ee80aefc4f22');

-- select * from User_t;


-- Table 2 Subscription_t contains details of subscriptions taken
DROP TABLE IF EXISTS Subscription_t;
CREATE TABLE Subscription_t (
    SubscriptionID INT PRIMARY KEY AUTO_INCREMENT,
    SubscriptionStartDate DATE,
    SubscriptionEndDate DATE,
    Duration INT,
    CostOfSubscription DECIMAL(10 , 2),
    UserID INT,
    FOREIGN KEY (UserID)
        REFERENCES User_t (UserID)
)  AUTO_INCREMENT=2001;

INSERT INTO Subscription_t (SubscriptionID, SubscriptionStartDate, SubscriptionEndDate, Duration, CostOfSubscription, UserID) VALUES
(2001, '2023-08-14', '2023-09-21', 240, 24.00, 1020),
(2002, '2024-04-22', '2024-04-27', 724, 72.40, 1020),
(2003, '2023-05-29', '2023-06-28', 150, 15.00, 1020),
(2004, '2023-07-02', '2023-08-12', 168, 16.80, 1020),
(2005, '2023-09-22', '2023-12-29', 190, 19.00, 1020),
(2006, '2023-12-30', '2024-01-02', 69, 6.90, 1020),
(2007, '2024-01-03', '2024-03-14', 154, 15.40, 1020),
(2008, '2024-03-15', '2024-04-20', 210, 21.00, 1020),
(2009, '2023-05-15', '2023-07-17', 131, 13.10, 1021),
(2010, '2023-11-19', '2023-11-29', 130, 13.00, 1021),
(2011, '2024-02-20', '2024-06-01', 210, 21.00, 1021),
(2012, '2023-12-07', '2024-01-01', 116, 11.60, 1021),
(2013, '2023-04-12', '2023-05-11', 111, 11.10, 1021),
(2014, '2023-09-23', '2023-09-29', 157, 15.70, 1021),
(2015, '2023-07-19', '2023-08-01', 122, 12.20, 1021),
(2016, '2024-01-06', '2024-01-28', 179, 17.90, 1021),
(2017, '2023-06-25', '2023-09-25', 128, 12.80, 1022),
(2018, '2023-11-01', '2023-11-10', 240, 24.00, 1022),
(2019, '2024-04-05', '2024-07-04', 289, 28.90, 1022),
(2020, '2023-10-10', '2023-10-12', 196, 19.60, 1022),
(2021, '2024-07-08', '2024-12-31', 149, 14.90, 1022),
(2022, '2023-01-19', '2023-01-23', 164, 16.40, 1023),
(2023, '2023-03-08', '2023-03-19', 190, 19.00, 1023),
(2024, '2023-03-25', '2023-04-15', 115, 11.50, 1023),
(2025, '2024-04-12', '2024-04-25', 217, 21.70, 1023),
(2026, '2023-12-26', '2024-04-02', 215, 21.50, 1023),
(2027, '2024-04-03', '2024-04-09', 104, 10.40, 1023),
(2028, '2024-04-30', '2024-06-12', 175, 17.50, 1023),
(2029, '2023-01-28', '2023-03-05', 160, 16.00, 1023),
(2030, '2023-02-18', '2023-02-24', 188, 18.80, 1024),
(2031, '2023-08-25', '2023-12-31', 141, 14.10, 1024),
(2032, '2024-01-28', '2024-02-27', 175, 17.50, 1024),
(2033, '2024-01-09', '2024-01-15', 159, 15.90, 1024),
(2034, '2023-03-31', '2023-06-01', 315, 31.50, 1024),
(2035, '2024-01-02', '2024-01-06', 157, 15.70, 1024),
(2036, '2023-07-14', '2023-07-24', 151, 15.10, 1024),
(2037, '2023-02-25', '2023-02-28', 50, 5.00, 1024),
(2038, '2024-02-05', '2024-02-06', 210, 21.00, 1025),
(2039, '2024-03-11', '2024-03-15', 810, 81.00, 1025),
(2040, '2024-07-25', '2024-08-28', 332, 33.20, 1025),
(2041, '2023-08-26', '2023-08-28', 211, 21.10, 1025),
(2042, '2024-03-25', '2024-03-26', 60, 6.00, 1025),
(2043, '2024-02-07', '2024-02-14', 148, 14.80, 1025),
(2044, '2024-04-15', '2024-05-18', 179, 17.90, 1025),
(2045, '2024-05-21', '2024-06-01', 179, 17.90, 1025),
(2046, '2024-03-16', '2024-03-17', 180, 18.00, 1025),
(2047, '2023-06-21', '2023-06-22', 183, 18.30, 1025),
(2048, '2023-12-21', '2024-01-14', 174, 17.40, 1025),
(2049, '2024-01-15', '2024-01-18', 179, 17.90, 1025),
(2050, '2024-05-19', '2024-05-20',112, 17.30, 1025);


-- select * from Subscription_t;



-- Table 3 Course_t contains catalog of courses offered 
DROP TABLE IF EXISTS Course_t;
CREATE TABLE Course_t (
    CourseID INT PRIMARY KEY AUTO_INCREMENT,
    CourseName VARCHAR(100) NOT NULL,
    ModeOfCourse VARCHAR(50),
    CourseCost DECIMAL(10 , 2 ),
    CourseCategory VARCHAR(50)
)  AUTO_INCREMENT=3001;


-- select * from Course_t 


INSERT INTO Course_t (CourseName, ModeOfCourse, CourseCost, CourseCategory)
VALUES
('Advanced Data Analysis', 'Online', 7.50, 'Technology'),
('Introduction to Public Speaking', 'Online', 9.25, 'Personal Development'),
('Project Management Fundamentals', 'Online', 8.75, 'Technology'),
('Python Programming Basics', 'Online', 6.80, 'Technology'),
('Painting for Beginners', 'Online', 5.50, 'Hobby'),
('Advanced Excel Techniques', 'Online', 8.90, 'Technology'),
('Time Management Strategies', 'Online', 7.75, 'Personal Development'),
('Introduction to Machine Learning', 'Online', 9.00, 'Technology'),
('Photography Fundamentals', 'Online', 7.20, 'Hobby'),
('Financial Planning Basics', 'Online', 6.30, 'Personal Development'),
('Critical Thinking Skills', 'Online', 8.10, 'Personal Development'),
('Cybersecurity', 'Online', 9.50, 'Technology'),
('Cooking Essentials', 'Online', 5.80, 'Hobby'),
('Natural Language Processing', 'Online', 8.40, 'Technology'),
('Interview Guidance', 'Online', 7.90, 'Personal Development'),
('Cloud Computing', 'Online', 8.70, 'Technology'),
('Java Programming Fundamentals', 'Online', 6.60, 'Technology'),
('Fishing Basics', 'Online', 4.90, 'Hobby'),
('Leadership Skills Development', 'Online', 9.30, 'Personal Development'),
('Introduction to Data Science', 'Online', 8.80, 'Technology'),
('Basic Carpentry Techniques', 'Online', 5.70, 'Hobby'),
('Financial Investment Strategies', 'Online', 7.40, 'Personal Development'),
('Creative Writing Workshop', 'Online', 6.20, 'Personal Development'),
('Database Management Essentials', 'Online', 8.20, 'Technology'),
('Yoga and Meditation Techniques', 'Online', 6.40, 'Personal Development'),
('Supply Chain Management Basics', 'Online', 8.60, 'Technology'),
('Digital Marketing Fundamentals', 'Online', 7.30, 'Technology'),
('Sustainability', 'Online', 6.00, 'Hobby'),
('Communication Skills Enhancement', 'Online', 9.20, 'Personal Development'),
('Introduction to Blockchain', 'Online', 9.10, 'Technology'),
('Basic Knitting Techniques', 'Online', 5.60, 'Hobby'),
('Strategic Planning in Business', 'Online', 8.30, 'Personal Development'),
('Artificial Intelligence Fundamentals', 'Online', 7.60, 'Technology'),
('Music Production Basics', 'Online', 6.80, 'Hobby'),
('Entrepreneurship Skills Development', 'Online', 9.40, 'Personal Development'),
('Basic Electrical Wiring Techniques', 'Online', 5.90, 'Hobby'),
('Customer Service Excellence', 'Online', 8.50, 'Personal Development'),
('Resume Preparation', 'Online', 7.70, 'Personal Development'),
('Financial Planning Advanced Concepts', 'Online', 6.50, 'Personal Development'),
('Problem Solving Skills', 'Online', 8.00, 'Personal Development'),
('Introduction to Web Development', 'Online', 5.40, 'Technology'),
('Swimming', 'Online', 7.10, 'Hobby'),
('Data Visualization Techniques', 'Online', 6.30, 'Technology'),
('Emotional Intelligence Training', 'Online', 8.80, 'Personal Development'),
('Project Risk Management', 'Online', 5.20, 'Technology'),
('MongoDB', 'Online', 7.60, 'Technology'),
('Gardening Basics', 'Online', 6.90, 'Hobby'),
('Stress Management', 'Online', 8.30, 'Personal Development'),
('Introduction to Time Series', 'Online', 5.80, 'Technology'),
('Database Foundations for Business Analytics','Online', 5.0, 'Technology');





-- Table 4 Course_Discount_t contains discounts that can be applied to courses offered on the platform
DROP TABLE IF EXISTS Course_Discount_t;
CREATE TABLE Course_Discount_t (
    CouponID INT PRIMARY KEY AUTO_INCREMENT,
    CouponCode VARCHAR(50) UNIQUE,
    Discount_Percentage DECIMAL(5 , 2 ),
    CouponStartDate DATE,
    CouponEndDate DATE
)  AUTO_INCREMENT=4001;



INSERT INTO Course_Discount_t (CouponCode, Discount_Percentage, CouponStartDate, CouponEndDate) VALUES
('GROW10OFF', 10.00, '2023-01-01', '2023-01-31'),
('EXPLORE15OFF', 15.00, '2023-02-01', '2023-02-28'),
('ACHIEVE20OFF', 20.00, '2023-03-01', '2023-03-31'),
('DEVELOP12OFF', 12.00, '2023-04-01', '2023-04-30'),
('MASTER8OFF', 8.00, '2023-05-01', '2023-05-31'),
('PROGRESS18OFF', 18.00, '2023-06-01', '2023-06-30'),
('SKILL5OFF', 5.00, '2023-07-01', '2023-07-31'),
('ADVANCE13OFF', 13.00, '2023-08-01', '2023-08-31'),
('ENRICH7OFF', 7.00, '2023-09-01', '2023-09-30'),
('EMPOWER11OFF', 11.00, '2023-10-01', '2023-10-31'),
('INSPIRE17OFF', 17.00, '2023-11-01', '2023-11-30'),
('EDUCATE3OFF', 3.00, '2023-12-01', '2023-12-31'),
('BOOST9OFF', 9.00, '2024-01-01', '2024-01-31'),
('EXCEL14OFF', 14.00, '2024-02-01', '2024-02-29'),
('SUCCEED19OFF', 19.00, '2024-03-01', '2024-03-31'),
('ACCOMPLISH6OFF', 6.00, '2024-04-01', '2024-04-30'),
('THRIVE16OFF', 16.00, '2024-05-01', '2024-05-31'),
('LEARN4OFF', 4.00, '2024-06-01', '2024-06-30'),
('EXCEL2OFF', 2.00, '2024-07-01', '2024-07-31'),
('IMPACT1OFF', 1.00, '2024-08-01', '2024-08-31'),
('DISCOVER18OFF', 18.00, '2024-09-01', '2024-09-30'),
('ENLIGHTEN5OFF', 5.00, '2024-10-01', '2024-10-31'),
('EXPAND12OFF', 12.00, '2024-11-01', '2024-11-30'),
('RISE3OFF', 3.00, '2024-12-01', '2024-12-31'),
('THRIVE10OFF', 10.00, '2025-01-01', '2025-01-31'),
('DEVELOP15OFF', 15.00, '2025-02-01', '2025-02-28'),
('EVOLVE20OFF', 20.00, '2025-03-01', '2025-03-31'),
('LEAD7OFF', 7.00, '2025-04-01', '2025-04-30'),
('PROMOTE11OFF', 11.00, '2025-05-01', '2025-05-31'),
('ENHANCE16OFF', 16.00, '2025-06-01', '2025-06-30'),
('INSIGHT6OFF', 6.00, '2025-07-01', '2025-07-31'),
('EXCEED14OFF', 14.00, '2025-08-01', '2025-08-31'),
('DEVELOP9OFF', 9.00, '2025-09-01', '2025-09-30'),
('GROWTH13OFF', 13.00, '2025-10-01', '2025-10-31'),
('TRANSFORM4OFF', 4.00, '2025-11-01', '2025-11-30'),
('PROGRESS2OFF', 2.00, '2025-12-01', '2025-12-31'),
('ENHANCE8OFF', 8.00, '2026-01-01', '2026-01-31'),
('EVOLVE10OFF', 10.00, '2026-02-01', '2026-02-28'),
('LEARNMORE15OFF', 15.00, '2026-03-01', '2026-03-31'),
('PROGRESS7OFF', 7.00, '2026-04-01', '2026-04-30'),
('ACHIEVE11OFF', 11.00, '2026-05-01', '2026-05-31'),
('DISCOVER20OFF', 20.00, '2026-06-01', '2026-06-30'),
('GROWTH9OFF', 9.00, '2026-07-01', '2026-07-31'),
('INSIGHT12OFF', 12.00, '2026-08-01', '2026-08-31'),
('INSPIRE5OFF', 5.00, '2026-09-01', '2026-09-30'),
('ADVANCE25OFF', 25.00, '2026-10-01', '2026-10-31'),
('DEVELOP6OFF', 6.00, '2026-11-01', '2026-11-30'),
('PROGRESS3OFF', 3.00, '2026-12-01', '2026-12-31'),
('ENLIGHTEN16OFF', 16.00, '2027-01-01', '2027-01-31'),
('EXPLORE2OFF', 2.00, '2027-02-01', '2027-02-28');

-- select * from Course_Discount_t;



-- Table 5 Content_Creator_t contains details of course instructors
DROP TABLE IF EXISTS Content_Creator_t;
CREATE TABLE Content_Creator_t (
    CreatorID INT PRIMARY KEY AUTO_INCREMENT,
    Creator_Name VARCHAR(100) NOT NULL,
    Designation VARCHAR(50),
    Experience INT,
    CourseID INT,
    FOREIGN KEY (CourseID)
        REFERENCES Course_t (CourseID)
)  AUTO_INCREMENT=5001;

-- Add a sequence starting from 5001
ALTER TABLE Content_Creator_t AUTO_INCREMENT = 5001;

-- Insert the values into the table with the sequence
INSERT INTO Content_Creator_t (Creator_Name, Designation, Experience,CourseID) VALUES
('Alice Johnson', 'Asst. Professor', 5,3001),
('Bob Smith', 'Professor', 8,3002),
('Charlie Brown', 'Industry Expert', 10,3003),
('David Miller', 'Asst. Professor', 6,3004),
('Emma Davis', 'Professor',  9,3005),
('Frank Wilson', 'Industry Expert',  12,3006),
('Grace Taylor', 'Asst. Professor',  4,3007),
('Hannah Martinez', 'Professor',  7,3008),
('Ian Anderson', 'Industry Expert',  11,3009),
('Jessica Thomas', 'Asst. Professor',  5,3010),
('Kevin Garcia', 'Professor',  8,3011),
('Lily Clark', 'Industry Expert',  10,3012),
('Megan White', 'Asst. Professor',  3,3013),
('Nathan Harris', 'Professor',  7,3014),
('Olivia Thompson', 'Industry Expert',9,3015),
('Peter King', 'Asst. Professor',  4,3016),
('Rachel Lee', 'Professor',  6,3017),
('Samuel Young', 'Industry Expert',  11,3018),
('Taylor Moore', 'Asst. Professor',  3,3019),
('Uma Patel', 'Professor',  7,3020),
('Victor Rivera', 'Industry Expert',  10,3021),
('Wendy Scott', 'Asst. Professor',  4,3022),
('Xavier Baker', 'Professor',  8,3023),
('Yvonne Nguyen', 'Industry Expert',  12,3024),
('Zoe Hughes', 'Asst. Professor',  5,3025),
('Adam Carter', 'Professor',  8,3026),
('Benjamin Evans', 'Industry Expert',  11,3027),
('Catherine Allen', 'Asst. Professor',  4,3028),
('Daniel Brown', 'Professor',  7,3029),
('Emily Cooper', 'Industry Expert',  10,3030),
('Felix Morris', 'Asst. Professor',  3,3031),
('Gabrielle Hall', 'Professor', 6,3032),
('Henry Wright', 'Industry Expert',  12,3033),
('Isabella Adams', 'Asst. Professor', 4,3034),
('Jack Turner', 'Professor', 8,3035),
('Katherine Roberts', 'Industry Expert',  11,3036),
('Liam Hughes', 'Asst. Professor',  5,3037),
('Madison Scott', 'Professor',  7,3038),
('Natalie Baker', 'Industry Expert',  10,3039),
('Oscar Hill', 'Asst. Professor',  4,3040),
('Paige Phillips', 'Professor',  8,3041),
('Quinn Ross', 'Industry Expert',  12,3042),
('Brenda Siri', 'Asst. Professor',  3,3043),
('Avanthi Sethi', 'Professor',  6,3044),
('Ali Razvi', 'Industry Expert',  11,3045),
('Syam Menon', 'Asst. Professor',  5,3046),
('Kannan Srikanth', 'Professor',  7,3047),
('B P Murthi','Professor', 12,3048),
('Gaurav Shekhar','Asst. Professor',9,3049),
('Judd Bradbury','Professor',10,3050);


-- select * from Content_Creator_t;


-- Table 6 ContentCreator_Payroll_t contains details of the payroll for the course instructors
DROP TABLE IF EXISTS ContentCreator_Payroll_t;
CREATE TABLE ContentCreator_Payroll_t (
    ContentCreator_PayrollID INT PRIMARY KEY AUTO_INCREMENT,
    CreatorID INT,
    StartDate DATE,
    EndDate DATE,
    Salary INT,
    FOREIGN KEY (CreatorID)
        REFERENCES Content_Creator_t (CreatorID)
)  AUTO_INCREMENT=6001;

INSERT INTO ContentCreator_Payroll_t (CreatorID, StartDate, EndDate, Salary) VALUES
(5001, '2024-01-01', '2024-01-31', 5000),
(5001, '2024-02-01', '2024-02-29', 5000),
(5001, '2024-03-01', '2024-03-31', 5000),
(5001, '2024-04-01', '2024-04-30', 5000),
(5001, '2024-05-01', '2024-05-31', 5000),
(5002, '2024-01-01', '2024-01-31', 5500),
(5002, '2024-02-01', '2024-02-29', 5500),
(5002, '2024-03-01', '2024-03-31', 5500),
(5002, '2024-04-01', '2024-04-30', 5500),
(5002, '2024-05-01', '2024-05-31', 5500),
(5003, '2024-01-01', '2024-01-31', 6000),
(5003, '2024-02-01', '2024-02-29', 6000),
(5003, '2024-03-01', '2024-03-31', 6000),
(5003, '2024-04-01', '2024-04-30', 6000),
(5003, '2024-05-01', '2024-05-31', 6000),
(5004, '2024-01-01', '2024-01-31', 6500),
(5004, '2024-02-01', '2024-02-29', 6500),
(5004, '2024-03-01', '2024-03-31', 6500),
(5004, '2024-04-01', '2024-04-30', 6500),
(5004, '2024-05-01', '2024-05-31', 6500),
(5005, '2024-01-01', '2024-01-31', 7000),
(5005, '2024-02-01', '2024-02-29', 7000),
(5005, '2024-03-01', '2024-03-31', 7000),
(5005, '2024-04-01', '2024-04-30', 7000),
(5005, '2024-05-01', '2024-05-31', 7000),
(5006, '2024-01-01', '2024-01-31', 7500),
(5006, '2024-02-01', '2024-02-29', 7500),
(5006, '2024-03-01', '2024-03-31', 7500),
(5006, '2024-04-01', '2024-04-30', 7500),
(5006, '2024-05-01', '2024-05-31', 7500),
(5007, '2024-01-01', '2024-01-31', 8000),
(5007, '2024-02-01', '2024-02-29', 8000),
(5007, '2024-03-01', '2024-03-31', 8000),
(5007, '2024-04-01', '2024-04-30', 8000),
(5007, '2024-05-01', '2024-05-31', 8000),
(5008, '2024-01-01', '2024-01-31', 8500),
(5008, '2024-02-01', '2024-02-29', 8500),
(5008, '2024-03-01', '2024-03-31', 8500),
(5008, '2024-04-01', '2024-04-30', 8500),
(5008, '2024-05-01', '2024-05-31', 8500),
(5009, '2024-01-01', '2024-01-31', 9000),
(5009, '2024-02-01', '2024-02-29', 9000),
(5009, '2024-03-01', '2024-03-31', 9000),
(5009, '2024-04-01', '2024-04-30', 9000),
(5009, '2024-05-01', '2024-05-31', 9000),
(5010, '2024-01-01', '2024-01-31', 9500),
(5010, '2024-02-01', '2024-02-29', 9500),
(5010, '2024-03-01', '2024-03-31', 9500),
(5010, '2024-04-01', '2024-04-30', 9500),
(5010, '2024-05-01', '2024-05-31', 9500);

-- select * from ContentCreator_Payroll_t;

-- Table 7 Advertising_t contains details of advertising campaigns promoting the platform
DROP TABLE IF EXISTS Advertising_t;
CREATE TABLE Advertising_t (
    AdvertisementID INT PRIMARY KEY AUTO_INCREMENT,
    PlatformOfAdvertising VARCHAR(100),
    CostOfAdvertising DECIMAL(10 , 2 ),
    AdvertisementStartDate DATE,
    AdvertisementEndDate DATE
)  AUTO_INCREMENT=7001;

INSERT INTO Advertising_t (PlatformOfAdvertising, CostOfAdvertising, AdvertisementStartDate, AdvertisementEndDate) VALUES
('Social Media', 3.50, '2023-01-05', '2023-01-15'),
('Online Banner', 2.75, '2023-02-12', '2023-02-20'),
('Search Engine', 4.20, '2023-03-20', '2023-03-30'),
('Television', 1.80, '2023-04-08', '2023-04-18'),
('Radio', 3.25, '2023-05-01', '2023-05-10'),
('Print Media', 2.40, '2023-06-10', '2023-06-20'),
('Billboard', 1.50, '2023-07-15', '2023-07-25'),
('Website Sponsorship', 4.90, '2023-08-20', '2023-08-30'),
('Email Marketing', 3.75, '2023-09-25', '2023-10-05'),
('Influencer Partnership', 2.10, '2023-10-30', '2023-11-09'),
('Product Placement', 4.30, '2023-11-15', '2023-11-25'),
('Event Sponsorship', 1.25, '2023-12-20', '2023-12-30'),
('Social Media', 3.10, '2024-01-05', '2024-01-15'),
('Online Banner', 2.95, '2024-02-10', '2024-02-20'),
('Search Engine', 4.50, '2024-03-15', '2024-03-25'),
('Television', 1.60, '2024-04-20', '2024-04-30'),
('Radio', 3.35, '2024-05-25', '2024-06-04'),
('Print Media', 2.20, '2024-06-30', '2024-07-10'),
('Billboard', 1.75, '2024-07-15', '2024-07-25'),
('Website Sponsorship', 4.70, '2024-08-20', '2024-08-30'),
('Email Marketing', 3.85, '2024-09-25', '2024-10-05'),
('Influencer Partnership', 2.30, '2024-10-30', '2024-11-09'),
('Product Placement', 4.10, '2024-11-15', '2024-11-25'),
('Event Sponsorship', 1.40, '2024-12-20', '2024-12-30'),
('Social Media', 3.25, '2025-01-05', '2025-01-15'),
('Online Banner', 2.80, '2025-02-10', '2025-02-20'),
('Search Engine', 4.20, '2025-03-15', '2025-03-25'),
('Television', 1.90, '2025-04-20', '2025-04-30'),
('Radio', 3.15, '2025-05-25', '2025-06-04'),
('Print Media', 2.30, '2025-06-30', '2025-07-10'),
('Billboard', 1.55, '2025-07-15', '2025-07-25'),
('Website Sponsorship', 4.60, '2025-08-20', '2025-08-30'),
('Email Marketing', 3.95, '2025-09-25', '2025-10-05'),
('Influencer Partnership', 2.20, '2025-10-30', '2025-11-09'),
('Product Placement', 4.20, '2025-11-15', '2025-11-25'),
('Event Sponsorship', 1.35, '2025-12-20', '2025-12-30'),
('Social Media', 3.40, '2023-01-05', '2023-01-15'),
('Online Banner', 2.85, '2023-02-10', '2023-02-20'),
('Search Engine', 4.30, '2023-03-15', '2023-03-25'),
('Television', 1.70, '2023-04-20', '2023-04-30'),
('Radio', 3.05, '2023-05-25', '2023-06-04'),
('Print Media', 2.40, '2023-06-30', '2023-07-10'),
('Billboard', 1.65, '2023-07-15', '2023-07-25'),
('Website Sponsorship', 4.50, '2023-08-20', '2023-08-30'),
('Email Marketing', 3.75, '2023-09-25', '2023-10-05'),
('Influencer Partnership', 2.25, '2023-10-30', '2023-11-09'),
('Product Placement', 4.15, '2023-11-15', '2023-11-25'),
('Event Sponsorship', 1.30, '2023-12-20', '2023-12-30'),
('Social Media', 3.20, '2024-12-01', '2024-12-31'),
('Online Banner', 2.50, '2025-02-01', '2025-02-10');

-- select * from Advertising_t;

-- Table 8 Events_t contains information about the free demo sessions in order to promote courses
DROP TABLE IF EXISTS Events_t;
CREATE TABLE Events_t (
    EventID INT PRIMARY KEY AUTO_INCREMENT,
    EventName VARCHAR(100),
    CourseID INT,
    EventDate DATE,
    FOREIGN KEY (CourseID)
        REFERENCES Course_t (CourseID)
)  AUTO_INCREMENT=8001;

INSERT INTO Events_t (EventName, CourseID, EventDate) VALUES
('Webinar on Data Science', 3020, '2024-01-15'),
('Digital Marketing Webinar', 3027, '2024-02-20'),
('Introduction to AI Webinar', 3033, '2024-03-10'),
('Webinar on Cybersecurity', 3012, '2024-04-05'),
('UX Design Webinar', 3041, '2024-05-01'),
('Blockchain Technology Webinar', 3030, '2024-06-18'),
('E-commerce Strategies Webinar', 3027, '2024-07-23'),
('Cloud Computing Webinar', 3016, '2024-08-11'),
('Webinar on Digital Transformation', 3027, '2024-09-30'),
('Webinar on Machine Learning', 3008, '2024-10-25'),
('AI Hackathon', 3033, '2024-02-01'),
('Cybersecurity Hackathon', 3012, '2024-02-10'),
('Blockchain Hackathon', 3030, '2024-03-05'),
('SusTech Flow Event', 3028, '2024-04-20'),
('Data Science Hackathon', 3020, '2024-05-15'),
('Social Impact Hackathon', 3028, '2024-06-30'),
('SusTech Hackathon', 3028, '2024-07-10'),
('Sustainability Solutions Hackathon', 3028, '2024-08-05'),
('Finance Tech Hackathon', 3022, '2024-09-20'),
('Environmental Sustainability Hackathon', 3028, '2024-10-15'),
('Webinar on Digital Marketing Trends', 3027, '2024-03-01'),
('AI in Business Webinar', 3033, '2024-03-15'),
('Webinar on UX/UI Design', 3041, '2024-04-10'),
('Cybersecurity Essentials Webinar', 3012, '2024-05-05'),
('Blockchain Applications Webinar', 3030, '2024-06-20'),
('E-commerce Strategies Webinar', 3027, '2024-07-15'),
('Webinar on Cloud Computing', 3016, '2024-08-30'),
('Digital Transformation Webinar', 3027, '2024-09-15'),
('AI & ML Hackathon', 3033, '2024-02-20'),
('Fintech Hackathon', 3022, '2024-03-05'),
('Sustainability Tech Hackathon', 3028, '2024-04-15'),
('Blockchain Hackathon', 3030, '2024-05-10'),
('Cybersecurity Challenge', 3012, '2024-06-25'),
('Smart City Hackathon', 3028, '2024-07-20'),
('SusTech Hackathon', 3028, '2024-08-05'),
('Social Impact Hackathon', 3028, '2024-09-10'),
('Environmental Hackathon', 3028, '2024-10-20'),
('Data Science Hackathon', 3020, '2024-11-05'),
('AI & ML Hackathon', 3033, '2024-11-25'),
('Fintech Hackathon', 3022, '2024-12-10'),
('SusTech Hackathon', 3028, '2024-12-20'),
('Blockchain Hackathon', 3030, '2023-01-05'),
('Cybersecurity Challenge', 3012, '2023-01-20'),
('Smart City Hackathon', 3028, '2023-02-05'),
('EdTech Hackathon', 3001, '2023-02-15'),
('Social Impact Hackathon', 3028, '2023-03-01'),
('Environmental Hackathon', 3028, '2023-03-15'),
('Data Science Hackathon', 3020, '2023-04-05'),
('AI & ML Hackathon', 3033, '2023-04-20'),
('Fintech Hackathon', 3022, '2023-05-05');



-- select * from events_t;







-- Table 9 SocialMedia_t contains details of the users logging into social media pages of the platform
DROP TABLE IF EXISTS SocialMedia_t;
CREATE TABLE SocialMedia_t (
    SocialMediaID INT PRIMARY KEY AUTO_INCREMENT,
    SocialMediaName VARCHAR(100),
    UserID INT,
    DateOfLogin DATE,
    FOREIGN KEY (UserID)
        REFERENCES User_t (UserID)
)  AUTO_INCREMENT=9001;

INSERT INTO SocialMedia_t (SocialMediaName, UserID, DateOfLogin) VALUES
('Facebook', 1001, '2024-01-01'),
('Instagram', 1001, '2024-01-02'),
('Twitter', 1001, '2024-01-03'),
('LinkedIn', 1001, '2024-01-04'),
('Pinterest', 1002, '2024-01-05'),
('Snapchat', 1002, '2024-01-06'),
('TikTok', 1002, '2024-01-07'),
('Reddit', 1002, '2024-01-08'),
('YouTube', 1002, '2024-01-09'),
('WhatsApp', 1015, '2024-01-10'),
('Facebook', 1015, '2024-01-11'),
('Instagram', 1015, '2024-01-12'),
('Twitter', 1015, '2024-01-13'),
('LinkedIn', 1017, '2024-01-14'),
('Pinterest', 1017, '2024-01-15'),
('Snapchat', 1017, '2024-01-16'),
('TikTok', 1017, '2024-01-17'),
('Reddit', 1017, '2024-01-18'),
('YouTube', 1025, '2024-01-19'),
('WhatsApp', 1025, '2024-01-20'),
('Facebook', 1025, '2024-01-21'),
('Instagram', 1025, '2024-01-22'),
('Twitter', 1025, '2024-01-23'),
('LinkedIn', 1025, '2024-01-24'),
('Pinterest', 1025, '2024-01-25'),
('Snapchat', 1026, '2024-01-26'),
('TikTok', 1026, '2024-01-27'),
('Reddit', 1026, '2024-01-28'),
('YouTube', 1026, '2024-01-29'),
('WhatsApp', 1026, '2024-01-30'),
('Facebook', 1031, '2024-01-31'),
('Instagram', 1032, '2024-02-01'),
('Twitter', 1033, '2024-02-02'),
('LinkedIn', 1034, '2024-02-03'),
('Pinterest', 1035, '2024-02-04'),
('Snapchat', 1036, '2024-02-05'),
('TikTok', 1037, '2024-02-06'),
('Reddit', 1038, '2024-02-07'),
('YouTube', 1039, '2024-02-08'),
('WhatsApp', 1040, '2024-02-09'),
('Facebook', 1041, '2024-02-10'),
('Instagram', 1042, '2024-02-11'),
('Twitter', 1043, '2024-02-12'),
('LinkedIn', 1044, '2024-02-13'),
('Pinterest', 1045, '2024-02-14'),
('Snapchat', 1046, '2024-02-15'),
('TikTok', 1047, '2024-02-16'),
('Reddit', 1048, '2024-02-17'),
('YouTube', 1049, '2024-02-18'),
('WhatsApp', 1050, '2024-02-19');

-- select * from socialMedia_t;




-- Table 10 Feedback_t contains information about the feedback provided to the courses
DROP TABLE IF EXISTS Feedback_t;
CREATE TABLE Feedback_t (
    FeedbackID INT PRIMARY KEY AUTO_INCREMENT,
    CourseID INT,
    UserID INT,
    FeedbackDescription TEXT,
    CreatorID INT,
    FeedbackRating DECIMAL(3 , 2 ),
    FeedbackDateTime TIMESTAMP,
    FOREIGN KEY (CourseID)
        REFERENCES Course_t (CourseID),
    FOREIGN KEY (UserID)
        REFERENCES User_t (UserID),
    FOREIGN KEY (CreatorID)
        REFERENCES Content_Creator_t (CreatorID)
)  AUTO_INCREMENT=10001;


INSERT INTO Feedback_t (UserID, CourseID, FeedbackDescription, CreatorID, FeedbackRating, FeedbackDateTime) VALUES
(1001, 3001, 'This course was excellent! I learned a lot and enjoyed every bit of it.', 5001, 4.8, '2024-01-01 00:00:00'),
(1001, 3002, 'The content was very informative and engaging. I highly recommend it!', 5002, 4.9, '2024-01-02 00:00:00'),
(1003, 3003, 'Amazing course! The instructor explained everything clearly with practical examples.', 5003, 4.7, '2024-01-03 00:00:00'),
(1005, 3004, 'The course was okay, but some parts were a bit confusing.', 5004, 3.5, '2024-01-04 00:00:00'),
(1005, 3004, 'I expected more depth in the content. It felt a bit shallow.', 5005, 3.2, '2024-01-05 00:00:00'),
(1005, 3004, 'Terrible course! Waste of time and money.', 5006, 1.5, '2024-01-06 00:00:00'),
(1005, 3007, 'The instructor was not knowledgeable and the material was outdated.', 5007, 1.8, '2024-01-07 00:00:00'),
(1005, 3008, 'One of the best courses I have ever taken. Highly recommended!', 5008, 4.9, '2024-01-08 00:00:00'),
(1006, 3009, 'Excellent content and presentation. Learned a lot from this course.', 5009, 4.8, '2024-01-09 00:00:00'),
(1007, 3010, 'Brilliant instructor! Made complex topics easy to understand.', 5010, 4.7, '2024-01-10 00:00:00'),
(1008, 3011, 'Fantastic course with practical exercises. Enjoyed every bit of it.', 5011, 4.9, '2024-01-11 00:00:00'),
(1009, 3012, 'The course content was decent, but the pacing could have been better.', 5012, 3.3, '2024-01-12 00:00:00'),
(1009, 3013, 'Not entirely satisfied with the course. Expected more depth.', 5013, 3.0, '2024-01-13 00:00:00'),
(1009, 3014, 'Poorly organized course. Instructor seemed unprepared.', 5014, 1.8, '2024-01-14 00:00:00'),
(1009, 3015, 'Waste of time. Would not recommend this course to anyone.', 5015, 1.5, '2024-01-15 00:00:00'),
(1009, 3016, 'I found the course to be very informative and engaging. The instructor was knowledgeable and explained concepts clearly.', 5016, 4.7, '2024-01-16 00:00:00'),
(1010, 3017, 'The course material was well-structured and easy to follow. I gained valuable insights from it.', 5017, 4.6, '2024-01-17 00:00:00'),
(1010, 3018, 'The course exceeded my expectations. The practical exercises were particularly helpful in understanding the concepts.', 5018, 4.9, '2024-01-18 00:00:00'),
(1010, 3019, 'Decent course overall, but some sections could have been explained better. Overall, satisfied with the learning experience.', 5019, 3.8, '2024-01-19 00:00:00'),
(1011, 3020, 'The course content was good, but the pace was too slow for my liking. Could have covered more topics in the given time.', 5020, 3.5, '2024-01-20 00:00:00'),
(1011, 3021, 'Disappointing course. The instructor lacked enthusiasm and the material felt outdated.', 5021, 2.1, '2024-01-21 00:00:00'),
(1011, 3022, 'Not satisfied with the course content. Expected more practical examples and real-world applications.', 5022, 2.5, '2024-01-22 00:00:00'),
(1011, 3023, 'The course was a waste of time. I did not learn anything useful from it.', 5023, 1.2, '2024-01-23 00:00:00'),
(1011, 3024, 'Poor quality course. The instructor seemed disinterested and the material was poorly explained.', 5024, 1.4, '2024-01-24 00:00:00'),
(1025, 3025, 'Horrible experience. The course was not worth the money.', 5025, 1.0, '2024-01-25 00:00:00'),
(1026, 3026, 'The course was average. Some parts were good, but overall, it lacked depth.', 5026, 3.0, '2024-01-26 00:00:00'),
(1027, 3027, 'Not impressed with the course content. Expected more practical knowledge and industry insights.', 5027, 2.8, '2024-01-27 00:00:00'),
(1028, 3028, 'The course was below expectations. The material felt outdated and not relevant.', 5028, 2.3, '2024-01-28 00:00:00'),
(1029, 3029, 'Mediocre course. The instructor was not engaging and the content lacked depth.', 5029, 3.2, '2024-01-29 00:00:00'),
(1030, 3030, 'The course was okay, but I expected more value for the price.', 5030, 3.5, '2024-01-30 00:00:00'),
(1031, 3031, 'Average course. The material could have been better organized and presented.', 5031, 3.0, '2024-01-31 00:00:00'),
(1032, 3032, 'The course content was comprehensive and well-structured. I learned a lot from it.', 5032, 4.9, '2024-01-01 00:00:00'),
(1033, 3033, 'Excellent course! The instructor was very knowledgeable and explained concepts clearly.', 5033, 4.8, '2024-01-02 00:00:00'),
(1034, 3034, 'Highly recommended course! The material was engaging and easy to follow.', 5034, 4.7, '2024-01-03 00:00:00'),
(1035, 3035, 'One of the best courses I''ve taken. The practical examples helped reinforce learning.', 5035, 4.9, '2024-01-04 00:00:00'),
(1036, 3036, 'Fantastic learning experience! The course content exceeded my expectations.', 5036, 4.8, '2024-01-05 00:00:00'),
(1037, 3037, 'Brilliant course! The instructor''s teaching style was engaging and easy to follow.', 5037, 4.7, '2024-01-06 00:00:00'),
(1038, 3038, 'The course was excellent in every aspect. I''m very satisfied with the learning outcomes.', 5038, 4.9, '2024-01-07 00:00:00'),
(1039, 3039, 'The instructor was amazing and made complex topics understandable. Highly recommended!', 5039, 4.8, '2024-01-08 00:00:00'),
(1040, 3040, 'Outstanding course! I learned valuable skills that I can apply in my career.', 5040, 4.7, '2024-01-09 00:00:00'),
(1041, 3041, 'The course content was very relevant and up-to-date. It met all my expectations.', 5041, 4.9, '2024-01-10 00:00:00'),
(1042, 3042, 'Excellent course material with clear explanations. I thoroughly enjoyed the learning experience.', 5042, 4.8, '2024-01-11 00:00:00'),
(1043, 3043, 'Highly informative course! The instructor provided valuable insights throughout the course.', 5043, 4.7, '2024-01-12 00:00:00'),
(1044, 3044, 'The course exceeded my expectations. I would recommend it to anyone looking to learn new skills.', 5044, 4.9, '2024-01-13 00:00:00'),
(1045, 3045, 'Fantastic course content! The instructor''s expertise made learning enjoyable and engaging.', 5045, 4.8, '2024-01-14 00:00:00'),
(1046, 3046, 'I found the course to be very enriching and informative. It was worth every penny.', 5046, 4.7, '2024-01-15 00:00:00'),
(1047, 3047, 'Brilliant course! The material was well-presented and easy to understand.', 5047, 4.9, '2024-01-16 00:00:00'),
(1048, 3048, 'The instructor was excellent and provided clear explanations throughout the course.', 5048, 4.8, '2024-01-17 00:00:00'),
(1049, 3049, 'Outstanding course content! I learned a lot and enjoyed the interactive exercises.', 5049, 4.7, '2024-01-18 00:00:00'),
(1050, 3050, 'Excellent course material with practical examples. The instructor was very engaging.', 5050, 4.9, '2024-01-19 00:00:00');



-- Drop the table if it exists
DROP TABLE IF EXISTS Employee_t;

-- Create the table
CREATE TABLE Employee_t (
    EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeName VARCHAR(100) NOT NULL,
    Manager INT,
    Email VARCHAR(100),
    PhoneNumber VARCHAR(15),
    HireDate DATE,
    TerminationDate DATE,
    Designation VARCHAR(50),
    FOREIGN KEY (Manager)
        REFERENCES Employee_t (EmployeeID)
) AUTO_INCREMENT=11001;

INSERT INTO Employee_t (EmployeeName, Manager, Email, PhoneNumber, HireDate, TerminationDate, Designation) VALUES
('John Doe', NULL, 'john.doe@example.com', '123-456-7890', '2023-01-01', NULL, 'Software Engineer'),
('Alice Smith', 11001, 'alice.smith@example.com', '234-567-8901', '2023-01-15', NULL, 'Product Manager'),
('Bob Johnson', 11001, 'bob.johnson@example.com', '345-678-9012', '2023-02-01', NULL, 'Sales Representative'),
('Emily Brown', 11002, 'emily.brown@example.com', '456-789-0123', '2023-02-15', NULL, 'Marketing Specialist'),
('Michael Lee', 11002, 'michael.lee@example.com', '567-890-1234', '2023-03-01', NULL, 'Financial Analyst'),
('Sarah Williams', 11003, 'sarah.williams@example.com', '678-901-2345', '2023-03-15', NULL, 'HR Coordinator'),
('David Garcia', 11003, 'david.garcia@example.com', '789-012-3456', '2023-04-01', NULL, 'Graphic Designer'),
('Jennifer Martinez', 11004, 'jennifer.martinez@example.com', '890-123-4567', '2023-04-15', NULL, 'Customer Support Representative'),
('Daniel Rodriguez', 11004, 'daniel.rodriguez@example.com', '901-234-5678', '2023-05-01', NULL, 'Quality Assurance Tester'),
('Mary Davis', 11005, 'mary.davis@example.com', '012-345-6789', '2023-05-15', NULL, 'Software Developer'),
('Christopher Hernandez', 11005, 'christopher.hernandez@example.com', '123-456-7890', '2023-06-01', NULL, 'Business Analyst'),
('Linda Wilson', 11006, 'linda.wilson@example.com', '234-567-8901', '2023-06-15', NULL, 'UX Designer'),
('James Gonzalez', 11006, 'james.gonzalez@example.com', '345-678-9012', '2023-07-01', NULL, 'IT Specialist'),
('Patricia Anderson', 11007, 'patricia.anderson@example.com', '456-789-0123', '2023-07-15', NULL, 'Operations Manager'),
('Richard Martinez', 11007, 'richard.martinez@example.com', '567-890-1234', '2023-08-01', NULL, 'Technical Writer'),
('Susan Thomas', 11008, 'susan.thomas@example.com', '678-901-2345', '2023-08-15', NULL, 'Data Analyst'),
('Paul Taylor', 11008, 'paul.taylor@example.com', '789-012-3456', '2023-09-01', NULL, 'Web Developer'),
('Karen Hernandez', 11009, 'karen.hernandez@example.com', '890-123-4567', '2023-09-15', NULL, 'Accountant'),
('Steven Young', 11009, 'steven.young@example.com', '901-234-5678', '2023-10-01', NULL, 'Network Administrator'),
('Jessica King', 11010, 'jessica.king@example.com', '012-345-6789', '2023-10-15', NULL, 'Project Manager'),
('Matthew Lee', 11010, 'matthew.lee@example.com', '123-456-7890', '2023-11-01', NULL, 'Systems Analyst'),
('Nancy Clark', 11011, 'nancy.clark@example.com', '234-567-8901', '2023-11-15', NULL, 'Technical Support Specialist'),
('Karen Lewis', 11011, 'karen.lewis@example.com', '345-678-9012', '2023-12-01', NULL, 'Marketing Manager'),
('Timothy Walker', 11012, 'timothy.walker@example.com', '456-789-0123', '2023-12-15', NULL, 'HR Manager'),
('Betty Perez', 11012, 'betty.perez@example.com', '567-890-1234', '2024-01-01', NULL, 'Sales Manager'),
('Anthony Hall', 11013, 'anthony.hall@example.com', '678-901-2345', '2024-01-15', NULL, 'Customer Service Manager'),
('Dorothy Wright', 11013, 'dorothy.wright@example.com', '789-012-3456', '2024-02-01', NULL, 'Quality Assurance Manager'),
('Joseph Turner', 11014, 'joseph.turner@example.com', '890-123-4567', '2024-02-15', NULL, 'IT Manager'),
('Margaret Adams', 11014, 'margaret.adams@example.com', '901-234-5678', '2024-03-01', NULL, 'Operations Coordinator'),
('Ronald Baker', 11015, 'ronald.baker@example.com', '012-345-6789', '2024-03-15', NULL, 'Data Entry Clerk'),
('Andrew Hall', 11015, 'andrew.hall@example.com', '123-456-7890', '2024-04-01', NULL, 'Receptionist'),
('Gloria Green', 11016, 'gloria.green@example.com', '234-567-8901', '2024-04-15', NULL, 'Administrative Assistant'),
('Edward Cook', 11016, 'edward.cook@example.com', '345-678-9012', '2024-05-01', NULL, 'Office Manager'),
('Helen Phillips', 11017, 'helen.phillips@example.com', '456-789-0123', '2024-05-15', NULL, 'Executive Assistant'),
('Larry Bell', 11017, 'larry.bell@example.com', '567-890-1234', '2024-06-01', NULL, 'Legal Assistant'),
('Anna Diaz', 11018, 'anna.diaz@example.com', '678-901-2345', '2024-06-15', NULL, 'Human Resources Assistant'),
('Eric Reed', 11018, 'eric.reed@example.com', '789-012-3456', '2024-07-01', NULL, 'Marketing Assistant'),
('Martha Cox', 11019, 'martha.cox@example.com', '890-123-4567', '2024-07-15', NULL, 'Finance Assistant'),
('Stephen Howard', 11019, 'stephen.howard@example.com', '901-234-5678', '2024-08-01', NULL, 'Accounting Assistant'),
('Heather Ross', 11020, 'heather.ross@example.com', '012-345-6789', '2024-08-15', NULL, 'IT Assistant'),
('Keith Rivera', 11020, 'keith.rivera@example.com', '123-456-7890', '2024-09-01', NULL, 'Sales Assistant'),
('Laura Baker', 11021, 'laura.baker@example.com', '234-567-8901', '2024-09-15', NULL, 'Technical Writer'),
('Mark White', 11021, 'mark.white@example.com', '345-678-9012', '2024-10-01', NULL, 'Data Analyst'),
('Cynthia Garcia', 11022, 'cynthia.garcia@example.com', '456-789-0123', '2024-10-15', NULL, 'Web Developer'),
('Steven Martinez', 11022, 'steven.martinez@example.com', '567-890-1234', '2024-11-01', NULL, 'Accountant'),
('Deborah Lopez', 11023, 'deborah.lopez@example.com', '678-901-2345', '2024-11-15', NULL, 'Software Engineer'),
('Charles Hill', 11023, 'charles.hill@example.com', '789-012-3456', '2024-12-01', NULL, 'Customer Support Representative'),
('Sandra Scott', 11024, 'sandra.scott@example.com', '890-123-4567', '2024-12-15', NULL, 'Marketing Specialist'),
('Ryan Green', 11024, 'ryan.green@example.com', '901-234-5678', '2025-01-01', NULL, 'Financial Analyst'),
('Amy King', 11025, 'amy.king@example.com', '012-345-6789', '2025-01-15', NULL, 'HR Coordinator');





-- Table 12 Employee_Payroll_t contains information about payroll of the employees
DROP TABLE IF EXISTS Employee_Payroll_t;
CREATE TABLE Employee_Payroll_t (
    Employee_PayrollID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeID INT,
    StartDate DATE,
    EndDate DATE,
    Salary INT,
    FOREIGN KEY (EmployeeID)
        REFERENCES Employee_t (EmployeeID)
)  AUTO_INCREMENT=12001;



INSERT INTO Employee_Payroll_t (EmployeeID, StartDate, EndDate, Salary) VALUES
(11001, '2024-01-01', '2024-01-31', 5000),
(11001, '2024-02-01', '2024-02-29', 5000),
(11001, '2024-03-01', '2024-03-31', 5000),
(11001, '2024-04-01', '2024-04-30', 5000),
(11001, '2024-05-01', '2024-05-31', 5000),
(11002, '2024-01-01', '2024-01-31', 5500),
(11002, '2024-02-01', '2024-02-29', 5500),
(11002, '2024-03-01', '2024-03-31', 5500),
(11002, '2024-04-01', '2024-04-30', 5500),
(11002, '2024-05-01', '2024-05-31', 5500),
(11003, '2024-01-01', '2024-01-31', 6000),
(11003, '2024-02-01', '2024-02-29', 6000),
(11003, '2024-03-01', '2024-03-31', 6000),
(11003, '2024-04-01', '2024-04-30', 6000),
(11003, '2024-05-01', '2024-05-31', 6000),
(11004, '2024-01-01', '2024-01-31', 6500),
(11004, '2024-02-01', '2024-02-29', 6500),
(11004, '2024-03-01', '2024-03-31', 6500),
(11004, '2024-04-01', '2024-04-30', 6500),
(11004, '2024-05-01', '2024-05-31', 6500),
(11005, '2024-01-01', '2024-01-31', 7000),
(11005, '2024-02-01', '2024-02-29', 7000),
(11005, '2024-03-01', '2024-03-31', 7000),
(11005, '2024-04-01', '2024-04-30', 7000),
(11005, '2024-05-01', '2024-05-31', 7000),
(11006, '2024-01-01', '2024-01-31', 7500),
(11006, '2024-02-01', '2024-02-29', 7500),
(11006, '2024-03-01', '2024-03-31', 7500),
(11006, '2024-04-01', '2024-04-30', 7500),
(11006, '2024-05-01', '2024-05-31', 7500),
(11007, '2024-01-01', '2024-01-31', 8000),
(11007, '2024-02-01', '2024-02-29', 8000),
(11007, '2024-03-01', '2024-03-31', 8000),
(11007, '2024-04-01', '2024-04-30', 8000),
(11007, '2024-05-01', '2024-05-31', 8000),
(11008, '2024-01-01', '2024-01-31', 8500),
(11008, '2024-02-01', '2024-02-29', 8500),
(11008, '2024-03-01', '2024-03-31', 8500),
(11008, '2024-04-01', '2024-04-30', 8500),
(11008, '2024-05-01', '2024-05-31', 8500),
(11009, '2024-01-01', '2024-01-31', 9000),
(11009, '2024-02-01', '2024-02-29', 9000),
(11009, '2024-03-01', '2024-03-31', 9000),
(11009, '2024-04-01', '2024-04-30', 9000),
(11009, '2024-05-01', '2024-05-31', 9000),
(11010, '2024-01-01', '2024-01-31', 9500),
(11010, '2024-02-01', '2024-02-29', 9500),
(11010, '2024-03-01', '2024-03-31', 9500),
(11010, '2024-04-01', '2024-04-30', 9500),
(11010, '2024-05-01', '2024-05-31', 9500);



-- Table 13 Expenditure_t contains information about the expenditures incurred by the platform
DROP TABLE IF EXISTS Expenditure_t;
CREATE TABLE Expenditure_t (
    ExpenditureID INT PRIMARY KEY AUTO_INCREMENT,
    ExpenditureName VARCHAR(100),
    ExpenditureAmount DECIMAL(10 , 2 ),
    DateOfExpenditure DATE
)  AUTO_INCREMENT=13001;

INSERT INTO Expenditure_t (ExpenditureName, ExpenditureAmount, DateOfExpenditure) VALUES
('Office Supplies', 120.50, '2024-01-01'),
('Marketing Campaign', 250.75, '2024-01-02'),
('Software Subscriptions', 150.25, '2024-01-03'),
('Employee Salaries', 8500.00, '2024-01-04'),
('Travel Expenses', 420.30, '2024-01-05'),
('Utilities', 700.90, '2024-01-06'),
('Rent', 5000.00, '2024-01-07'),
('Training Costs', 1700.00, '2024-01-08'),
('Consulting Fees', 2200.60, '2024-01-09'),
('Software Licenses', 350.75, '2024-01-10'),
('Office Supplies', 150.50, '2024-01-11'),
('Marketing Campaign', 300.75, '2024-01-12'),
('Software Subscriptions', 180.25, '2024-01-13'),
('Employee Salaries', 8800.00, '2024-01-14'),
('Travel Expenses', 450.30, '2024-01-15'),
('Utilities', 720.90, '2024-01-16'),
('Rent', 5200.00, '2024-01-17'),
('Training Costs', 1900.00, '2024-01-18'),
('Consulting Fees', 2400.60, '2024-01-19'),
('Software Licenses', 400.75, '2024-01-20'),
('Office Supplies', 180.50, '2024-01-21'),
('Marketing Campaign', 350.75, '2024-01-22'),
('Software Subscriptions', 200.25, '2024-01-23'),
('Employee Salaries', 9000.00, '2024-01-24'),
('Travel Expenses', 480.30, '2024-01-25'),
('Utilities', 750.90, '2024-01-26'),
('Rent', 5500.00, '2024-01-27'),
('Training Costs', 2100.00, '2024-01-28'),
('Consulting Fees', 2600.60, '2024-01-29'),
('Software Licenses', 450.75, '2024-01-30'),
('Office Supplies', 200.50, '2024-01-31'),
('Marketing Campaign', 400.75, '2024-02-01'),
('Software Subscriptions', 220.25, '2024-02-02'),
('Employee Salaries', 9200.00, '2024-02-03'),
('Travel Expenses', 510.30, '2024-02-04'),
('Utilities', 780.90, '2024-02-05'),
('Rent', 5800.00, '2024-02-06'),
('Training Costs', 2300.00, '2024-02-07'),
('Consulting Fees', 2800.60, '2024-02-08'),
('Software Licenses', 500.75, '2024-02-09'),
('Office Supplies', 220.50, '2024-02-10'),
('Marketing Campaign', 450.75, '2024-02-11'),
('Software Subscriptions', 240.25, '2024-02-12'),
('Employee Salaries', 9500.00, '2024-02-13'),
('Travel Expenses', 540.30, '2024-02-14'),
('Utilities', 810.90, '2024-02-15'),
('Rent', 6000.00, '2024-02-16'),
('Training Costs', 2500.00, '2024-02-17'),
('Consulting Fees', 3000.60, '2024-02-18'),
('Software Licenses', 550.75, '2024-02-19');




-- Table 14 Enrolled_t contains information about the courses bought on the platform 
DROP TABLE IF EXISTS Enrolled_t;
CREATE TABLE Enrolled_t (
    EnrollmentID INT PRIMARY KEY AUTO_INCREMENT,
    EnrollmentDate DATE,
    UserID INT,
    CourseID INT,
    CreatorID INT,
    Tax DECIMAL(10 , 2 ),
    CouponID INT,
    FOREIGN KEY (UserID)
        REFERENCES User_t (UserID),
    FOREIGN KEY (CourseID)
        REFERENCES Course_t (CourseID),
    FOREIGN KEY (CreatorID)
        REFERENCES Content_Creator_t (CreatorID),
	FOREIGN KEY (CouponID)
		REFERENCES Course_Discount_t (CouponID)
)  AUTO_INCREMENT=14001;



-- Table 14 Enrolled_t contains information about the courses bought on the platform 
DROP TABLE IF EXISTS Enrolled_t;
CREATE TABLE Enrolled_t (
    EnrollmentID INT PRIMARY KEY AUTO_INCREMENT,
    EnrollmentDate DATE,
    UserID INT,
    CourseID INT,
    CreatorID INT,
    Tax DECIMAL(10, 2),
    FOREIGN KEY (UserID) REFERENCES User_t (UserID),
    FOREIGN KEY (CourseID) REFERENCES Course_t (CourseID),
    FOREIGN KEY (CreatorID) REFERENCES Content_Creator_t (CreatorID)
) AUTO_INCREMENT=14001;

INSERT INTO Enrolled_t (EnrollmentDate, UserID, CourseID, CreatorID, Tax) VALUES
('2024-01-01', 1001, 3001, 5001, NULL),
('2024-01-02', 1001, 3002, 5002, NULL),
('2024-01-03', 1003, 3003, 5003, NULL),
('2024-01-04', 1005, 3004, 5004, NULL),
('2024-01-05', 1005, 3004, 5005, NULL),
('2024-01-06', 1005, 3004, 5006, NULL),
('2024-01-07', 1005, 3007, 5007, NULL),
('2024-01-08', 1005, 3008, 5008, NULL),
('2024-01-09', 1006, 3009, 5009, NULL),
('2024-01-10', 1007, 3010, 5010, NULL),
('2024-01-11', 1008, 3011, 5011, NULL),
('2024-01-12', 1009, 3012, 5012, NULL),
('2024-01-13', 1009, 3013, 5013, NULL),
('2024-01-14', 1009, 3014, 5014, NULL),
('2024-01-15', 1009, 3015, 5015, NULL),
('2024-01-16', 1009, 3016, 5016, NULL),
('2024-01-17', 1010, 3017, 5017, NULL),
('2024-01-18', 1010, 3018, 5018, NULL),
('2024-01-19', 1010, 3019, 5019, NULL),
('2024-01-20', 1011, 3020, 5020, NULL),
('2024-01-21', 1011, 3021, 5021, NULL),
('2024-01-22', 1011, 3022, 5022, NULL),
('2024-01-23', 1011, 3023, 5023, NULL),
('2024-01-24', 1011, 3024, 5024, NULL),
('2024-01-25', 1025, 3025, 5025, NULL),
('2024-01-26', 1026, 3026, 5026, NULL),
('2024-01-27', 1027, 3027, 5027, NULL),
('2024-01-28', 1028, 3028, 5028, NULL),
('2024-01-29', 1029, 3029, 5029, NULL),
('2024-01-30', 1030, 3030, 5030, NULL),
('2024-01-31', 1031, 3031, 5031, NULL),
('2024-02-01', 1032, 3032, 5032, NULL),
('2024-02-02', 1033, 3033, 5033, NULL),
('2024-02-03', 1034, 3034, 5034, NULL),
('2024-02-04', 1035, 3035, 5035, NULL),
('2024-02-05', 1036, 3036, 5036, NULL),
('2024-02-06', 1037, 3037, 5037, NULL),
('2024-02-07', 1038, 3038, 5038, NULL),
('2024-02-08', 1039, 3039, 5039, NULL),
('2024-02-09', 1040, 3040, 5040, NULL),
('2024-02-10', 1041, 3041, 5041, NULL),
('2024-02-11', 1042, 3042, 5042, NULL),
('2024-02-12', 1043, 3043, 5043, NULL),
('2024-02-13', 1044, 3044, 5044, NULL),
('2024-02-14', 1045, 3045, 5045, NULL),
('2024-02-15', 1046, 3046, 5046, NULL),
('2024-02-16', 1047, 3047, 5047, NULL),
('2024-02-17', 1048, 3048, 5048, NULL),
('2024-02-18', 1049, 3049, 5049, NULL),
('2024-02-19', 1050, 3050, 5050, NULL);


-- Table 15 Course_Progression_t contains information about the course progress made by the end users
DROP TABLE IF EXISTS Course_Progression_t;
CREATE TABLE Course_Progression_t (
    SessionID INT PRIMARY KEY AUTO_INCREMENT,
    UserID INT,
    CourseID INT,
    CreatorID INT,
    Completion_status VARCHAR(20),
    Learning_hours DECIMAL(10 , 2 ),
    Progress DECIMAL(5 , 2 ),
    Last_login TIMESTAMP,
    FOREIGN KEY (UserID)
        REFERENCES User_t (UserID),
    FOREIGN KEY (CourseID)
        REFERENCES Course_t (CourseID),
    FOREIGN KEY (CreatorID)
        REFERENCES Content_Creator_t (CreatorID)
)  AUTO_INCREMENT=15001;


INSERT INTO Course_Progression_t (UserID, CourseID, CreatorID, Completion_status, Learning_hours, Progress, Last_login) VALUES
(1001, 3001, 5001, 'Completed', 25.5, 100, '2024-01-01 08:30:00'),
(1001, 3002, 5002, 'In Progress', 12.8, 65.5, '2024-01-02 09:45:00'),
(1003, 3003, 5003, 'Completed', 30.0, 100, '2024-01-03 10:15:00'),
(1005, 3004, 5004, 'Not Started', 0.0, 0, '2024-01-04 11:20:00'),
(1005, 3004, 5005, 'In Progress', 18.3, 75.5, '2024-01-05 12:10:00'),
(1005, 3004, 5006, 'Completed', 35.2, 100, '2024-01-06 13:25:00'),
(1005, 3007, 5007, 'Not Started', 0.0, 0, '2024-01-07 14:30:00'),
(1005, 3008, 5008, 'In Progress', 22.7, 88.5, '2024-01-08 15:45:00'),
(1006, 3009, 5009, 'Completed', 28.6, 100, '2024-01-09 16:10:00'),
(1007, 3010, 5010, 'In Progress', 14.5, 70.2, '2024-01-10 17:20:00'),
(1008, 3011, 5011, 'Not Started', 0.0, 0, '2024-01-11 18:30:00'),
(1009, 3012, 5012, 'In Progress', 19.8, 82.5, '2024-01-12 19:40:00'),
(1009, 3013, 5013, 'Completed', 32.0, 100, '2024-01-13 20:50:00'),
(1009, 3014, 5014, 'In Progress', 25.3, 93.8, '2024-01-14 21:00:00'),
(1009, 3015, 5015, 'Not Started', 0.0, 0, '2024-01-15 22:15:00'),
(1009, 3016, 5016, 'In Progress', 17.2, 78.5, '2024-01-16 23:30:00'),
(1010, 3017, 5017, 'Completed', 29.9, 100, '2024-01-17 01:00:00'),
(1010, 3018, 5018, 'In Progress', 21.5, 85.0, '2024-01-18 02:20:00'),
(1010, 3019, 5019, 'Not Started', 0.0, 0, '2024-01-19 03:45:00'),
(1011, 3020, 5020, 'In Progress', 16.4, 73.2, '2024-01-20 04:10:00'),
(1011, 3021, 5021, 'Completed', 31.7, 100, '2024-01-21 05:25:00'),
(1011, 3022, 5022, 'In Progress', 23.8, 88.5, '2024-01-22 06:30:00'),
(1011, 3023, 5023, 'Not Started', 0.0, 0, '2024-01-23 07:45:00'),
(1011, 3024, 5024, 'In Progress', 18.9, 75.5, '2024-01-24 08:50:00'),
(1025, 3025, 5025, 'Completed', 33.5, 100, '2024-01-25 09:55:00'),
(1026, 3026, 5026, 'In Progress', 27.6, 92.5, '2024-01-26 10:00:00'),
(1027, 3027, 5027, 'Not Started', 0.0, 0, '2024-01-27 11:15:00'),
(1028, 3028, 5028, 'In Progress', 20.7, 80.5, '2024-01-28 12:20:00'),
(1029, 3029, 5029, 'Completed', 35.0, 100, '2024-01-29 13:35:00'),
(1030, 3030, 5030, 'In Progress', 15.8, 68.2, '2024-01-30 14:40:00'),
(1031, 3031, 5031, 'Not Started', 0.0, 0, '2024-01-31 15:55:00'),
(1032, 3032, 5032, 'Completed', 28.3, 100, '2024-02-01 08:30:00'),
(1033, 3033, 5033, 'In Progress', 17.6, 78.5, '2024-02-02 09:45:00'),
(1034, 3034, 5034, 'Completed', 30.0, 100, '2024-02-03 10:15:00'),
(1035, 3035, 5035, 'Not Started', 0.0, 0, '2024-02-04 11:20:00'),
(1036, 3036, 5036, 'In Progress', 21.9, 85.5, '2024-02-05 12:10:00'),
(1037, 3037, 5037, 'Completed', 34.2, 100, '2024-02-06 13:25:00'),
(1038, 3038, 5038, 'Not Started', 0.0, 0, '2024-02-07 14:30:00'),
(1039, 3039, 5039, 'In Progress', 25.7, 92.5, '2024-02-08 15:45:00'),
(1040, 3040, 5040, 'Completed', 31.6, 100, '2024-02-09 16:10:00'),
(1041, 3041, 5041, 'In Progress', 18.5, 80.2, '2024-02-10 17:20:00'),
(1042, 3042, 5042, 'Not Started', 0.0, 0, '2024-02-11 18:30:00'),
(1043, 3043, 5043, 'In Progress', 23.8, 88.5, '2024-02-12 19:40:00'),
(1044, 3044, 5044, 'Completed', 29.0, 100, '2024-02-13 20:50:00'),
(1045, 3045, 5045, 'In Progress', 26.3, 95.8, '2024-02-14 21:00:00'),
(1046, 3046, 5046, 'Not Started', 0.0, 0, '2024-02-15 22:15:00'),
(1047, 3047, 5047, 'In Progress', 19.2, 82.5, '2024-02-16 23:30:00'),
(1048, 3048, 5048, 'Completed', 32.9, 100, '2024-02-17 01:00:00'),
(1049, 3049, 5049, 'In Progress', 21.5, 85.0, '2024-02-18 02:20:00'),
(1050, 3050, 5050, 'Not Started', 0.0, 0, '2024-02-19 03:45:00');


show tables;

-- major queries
-- 1) Write a major query (view) to filter special characters and text characters in PhoneNumber field in the User_t table
select * from user_t;
set sql_safe_updates=0;
update user_t set PhoneNumber = REGEXP_REPLACE(PhoneNumber,'[^0-9]','') where PhoneNumber REGEXP '[^0-9]';
select * from user_t;

-- 2) Write a major query to calculate the gaps days in subscription for the particular user
select * from Subscription_t;
select * from Subscription_t where userid=1023 order by  UserID asc,SubscriptionStartDate asc;


    SELECT 
        DATEDIFF(
            LEAD(SubscriptionStartDate) OVER (PARTITION BY UserID ORDER BY SubscriptionStartDate),
            SubscriptionEndDate
        ) AS GapDays
    FROM 
        Subscription_t
    WHERE 
        UserID = '1023';


-- 3) Write a major query to add row number sequence for Advertising_t 
select * from Advertising_t;
alter table Advertising_t add RowNumber int;
UPDATE Advertising_t 
JOIN (
    SELECT *, ROW_NUMBER() OVER (ORDER BY AdvertisementID) AS IndexID 
    FROM Advertising_t
) AS IndexedTable
ON Advertising_t.AdvertisementID = IndexedTable.AdvertisementID
SET Advertising_t.RowNumber = IndexedTable.IndexID;
select * from Advertising_t;


-- 4) Write a major query to get the course, event and content creator details which had the maximum number of events 
select * from Events_t order by CourseID;
select * from Content_Creator_t;
SELECT 
    Content_Creator_t.CourseID, 
    Events_t.EventName, 
    Content_Creator_t.Creator_Name 
FROM 
    Content_Creator_t 
JOIN 
Events_t ON Content_Creator_t.CourseID = Events_t.CourseID 
WHERE 
    Content_Creator_t.CourseID = (
        SELECT 
            CourseID
        FROM 
            Events_t
        GROUP BY 
            CourseID 
        ORDER BY 
            COUNT(*) DESC 
        LIMIT 1
    );
    
-- 5) Write a major query to get details of the employee along with their manager (employee_t)
select * from employee_t;
SELECT t1.EmployeeName, t2.EmployeeName AS ManagerName
FROM employee_t t1
JOIN employee_t t2 ON t1.Manager = t2.EmployeeID;

-- 6) Write a major query to perform union between Employee_payroll_t and Content_Creator_Payroll_t
select * from Employee_payroll_t;
select * from ContentCreator_Payroll_t;
select * from Employee_payroll_t union select * from  ContentCreator_Payroll_t;

-- 7) Write a major query to calculate rent from expenditure table 
select * from Expenditure_t ;
select ExpenditureName,sum(ExpenditureAmount) as TotalAmount from  Expenditure_t group by ExpenditureName having ExpenditureName = 'Rent';

-- 8) Write a major query to find Users who are subscribed but not enrolled to courses
select * from Subscription_t;
select * from Enrolled_t;
select * from Subscription_t s left join Enrolled_t e on s.UserID=e.UserID where EnrollmentID is NULL;
    
    
-- 9) Write a major query to find users who are both subscribed and enrolled in the month of January
select * from Subscription_t s inner join Enrolled_t e on s.UserID=e.UserID where MONTH(s.SubscriptionStartDate)=1 and MONTH(e.EnrollmentDate)=1;

-- 10) Write a major query to show enrolled_t details along with user, course, content creator details
select * from Enrolled_t;
select * from Course_t;
select * from Content_Creator_t;
select distinct e.UserID,c.CourseName,cc.Creator_Name from Enrolled_t e join Course_t c on e.CourseID=c.CourseID join Content_Creator_t cc on c.CourseID=cc.CourseID;

DELIMITER //
-- 1) Function to calculate the effective cost of the course( after discount) 
CREATE FUNCTION Calc_Effective_Course_Cost_F (CourseCost INT, Discount_Percentage INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE EffectiveCost DECIMAL(10,2);
    SET EffectiveCost = CourseCost *  (100 - Discount_Percentage)/100;
    RETURN EffectiveCost;
END //

DELIMITER ;

Select Calc_Effective_Course_Cost_F (10,10)

DELIMITER //
-- 2) Function to calculate the tax based on course cost
CREATE FUNCTION Calc_Tax_F (CourseCost INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE TaxCost DECIMAL(10,2);
    SET TaxCost = CourseCost *  0.18;
    RETURN TaxCost;
END //

DELIMITER ;
select Calc_Tax_F(10)

DELIMITER //
-- 3) Function to calculate the estimated cost based on duration of the subscription
CREATE FUNCTION Calc_EstimatedCost_Subscription_F (Duration INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE EstimatedCost DECIMAL(10,2);
    SET EstimatedCost = Duration *  0.1;
    RETURN EstimatedCost;
END //

DELIMITER ;

select Calc_EstimatedCost_Subscription_F (24)

DELIMITER //
-- 4) Write a function to calculate manager salary given employee id
CREATE FUNCTION Calc_ManagerSalary_F (EmployeeID_param INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE ManagerSalary DECIMAL(10,2);
    SELECT distinct t3.Salary into ManagerSalary
	FROM employee_t t1
	JOIN employee_t t2 ON t1.Manager = t2.EmployeeID 
    JOIN  Employee_Payroll_t t3 on t2.EmployeeID=t3.EmployeeID
    where t1.EmployeeID=EmployeeID_param;
    RETURN ManagerSalary;
END //

DELIMITER ;

select Calc_ManagerSalary_F(11010)

DELIMITER //

-- 5) Write a function to find if user has visited social media pages
CREATE FUNCTION Find_User_SocialMedia_F (UserID_param INT)
RETURNS VARCHAR(30)
DETERMINISTIC
BEGIN
    DECLARE FoundRecords INT;
    
    -- Check if any records are found in the query
    SELECT COUNT(*) INTO FoundRecords
    FROM user_t 
    JOIN socialmedia_t ON user_t.UserID = socialmedia_t.UserID
    WHERE user_t.UserID = UserID_param;
    
    -- Return 1 if records are found, 0 otherwise
    IF FoundRecords > 0 THEN
        RETURN 'User has Social Media presence';
    ELSE
        RETURN 'User does not have Social Media presence';
    END IF;
END //
DELIMITER ;

select Find_User_SocialMedia_F(1001)
select Find_User_SocialMedia_F(1010)

-- 1) stored procedure to return the effective course cost (after discount) and other course information for a given month and year 

DELIMITER // 
CREATE PROCEDURE Effective_Course_Cost_SP (year_param int, month_param int)
BEGIN
select course_t.*, Calc_Effective_Course_Cost_F(course_t.CourseCost, course_discount_t.Discount_Percentage) AS Discounted_Course_Cost  from course_t
join
course_discount_t on 1=1 
where YEAR(CouponStartDate)=year_param
and MONTH(CouponStartDate)=month_param;
END //
DELIMITER ;
CALL Effective_Course_Cost_SP(2023,2)

-- 2) Stored procedure to update the Tax based on the course cost. tax is 18% of the course cost, this procedure should also update the duration and cost of subscription in the Subscription table

DELIMITER // 
CREATE PROCEDURE Update_Tax_Duration_SubscriptionCost_SP ()
BEGIN
UPDATE Subscription_t 
set DURATION=DATEDIFF(SubscriptionendDate,SubscriptionStartDate);

UPDATE Subscription_t 
set CostOfSubscription = Calc_EstimatedCost_Subscription_F(Duration);


UPDATE Enrolled_t
JOIN Course_t ON Course_t.CourseID = Enrolled_t.CourseID
SET Enrolled_t.Tax = Calc_Tax_F(course_t.CourseCost);

END //
DELIMITER ;

CALL Update_Tax_Duration_SubscriptionCost_SP ();

select * from Subscription_t;
select * from Enrolled_t;

-- 3) Stored procedure to input feedback rating and get relevant feedback details 
DELIMITER // 
CREATE PROCEDURE Get_Feedback_Details_SP (feedback_rating_param decimal(18,2))
BEGIN
select * from feedback_t
where FeedbackRating=feedback_rating_param;

END //
DELIMITER ;
CALL Get_Feedback_Details_SP(4.90)

-- 4) Write a stored procedure to output the enrollment details  with month  and year as parameter
DELIMITER // 
CREATE PROCEDURE Get_Enrollment_Details_SP (month_param int, year_param int)
BEGIN
select * from enrolled_t
where MONTH(EnrollmentDate) = month_param and YEAR(EnrollmentDate) = year_param;

END //
DELIMITER ;
CALL Get_Enrollment_Details_SP(1,2024)


-- 5) Write a stored procedure to user details who have spent more than 20 learning hours or course progression greater than 80%
DELIMITER // 
CREATE PROCEDURE Get_User_Details_SP ()
BEGIN
select user_t.UserID, FirstName, Lastname, EmailID, Learning_hours, Progress,CourseName from user_t
JOIN Course_Progression_t on user_t.userid=Course_Progression_t.userid
JOIN Course_t on Course_t.CourseID=Course_Progression_t.CourseID
where Learning_hours > 20 or Progress > 80;
END //
DELIMITER ;
CALL Get_User_Details_SP ();




-- Triggers
/* Write a trigger to notify users that special characters and text based characters cannot be inserted in the PhoneNumber field */
DELIMITER //

CREATE TRIGGER CheckPhoneNumber
BEFORE INSERT ON User_t
FOR EACH ROW
BEGIN
    -- Check if the PhoneNumber contains only digits (0-9)
    IF NEW.PhoneNumber REGEXP '[^0-9]' THEN
        -- error as an output if the PhoneNumber contains non-digits
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Special characters and text based characters cannot be inserted in the PhoneNumber field.';
    END IF;
END //

DELIMITER ;

/* Insert Statement that generates the trigger */

 -- insert into user_t values(1052,'Ramya', 'Javvadi', 'ramya.j@example.com', '(914)998-9405x4', '72700 Viola Lock Apt. 244 South Gino, CT 00386', 'Female', '924eb91074bf8793ddafa01c868a77dc01471aea');




/*2. Write a trigger to insert/update trigger not to allow discount percentage to be greater than 30% for the course_discount_t table*/

DELIMITER //

CREATE TRIGGER BeforeUpdateDiscount
BEFORE UPDATE ON Course_Discount_t
FOR EACH ROW
BEGIN
    IF NEW.Discount_Percentage > 30 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Discount percentage should not be greater than 30%.';
    END IF;
END //

DELIMITER ;

/* update statement that invokes this trigger*/

-- update Course_Discount_t set Discount_Percentage=40 where CouponID= 4050;


/*3. Write a trigger for start_date always less than end date in the course_discount_t table */

DELIMITER //

CREATE TRIGGER CheckDateBeforeUpdate
BEFORE INSERT ON Course_Discount_t
FOR EACH ROW
BEGIN
    IF NEW.CouponStartDate >= NEW.CouponEndDate THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'The start date must be earlier than the end date.';
    END IF;
END //

DELIMITER ;

/* insert statement that invokes the trigger */

-- insert into Course_Discount_t values ( 4051,'GROW77OFF', 10.00, '2023-02-02', '2023-01-31');


/* 4. Track Course Completion and auto generate Certificate based on completion status*/

/* Creating certificates table , so that whenever user completes the course or completion status is updated , new record will be inserted in this table as trigger get invoked*/

CREATE TABLE Certificates_t (
    CertificateID INT PRIMARY KEY AUTO_INCREMENT,
    UserID INT,
    CourseID INT,
    IssueDate DATE,
    FOREIGN KEY (UserID) REFERENCES User_t (UserID),
    FOREIGN KEY (CourseID) REFERENCES Course_t (CourseID)
);

/* Table before trigger gets invoked */

select * from Certificates_t;  

DELIMITER //

CREATE TRIGGER GenerateCertificateAfterCompletion
AFTER UPDATE ON Course_Progression_t
FOR EACH ROW
BEGIN
    IF OLD.Completion_status <> 'Completed' AND NEW.Completion_status = 'Completed' THEN
        INSERT INTO Certificates_t (UserID, CourseID, IssueDate)
        VALUES (NEW.UserID, NEW.CourseID, CURDATE());
    END IF;
END //

DELIMITER ;


/* Update statement that invokes this trigger*/

-- update Course_Progression_t set Completion_status= 'Completed' where sessionID= 15049;

/* Table after Update */

-- select * from Certificates;  

/*5-  Trigger to Log Changes in Employee Status*/

/* Creating Employee audit table to record changes to log employee changes*/
CREATE TABLE EmployeeAudit_t (
    AuditID INT AUTO_INCREMENT PRIMARY KEY,
    EmployeeID INT,
    ChangeType VARCHAR(100),
    ChangeDate TIMESTAMP,
    PreviousValue VARCHAR(255),
    NewValue VARCHAR(255)
);

DELIMITER //

CREATE TRIGGER LogEmployeeChanges AFTER UPDATE ON Employee_t
FOR EACH ROW
BEGIN
    IF OLD.TerminationDate IS NULL AND NEW.TerminationDate IS NOT NULL THEN
        INSERT INTO EmployeeAudit_t (EmployeeID, ChangeType, ChangeDate, PreviousValue, NewValue)
        VALUES (NEW.EmployeeID, 'Termination', NOW(), CAST(OLD.TerminationDate AS CHAR), CAST(NEW.TerminationDate AS CHAR));
    END IF;
END //

DELIMITER ;


/* Update statement that will invoke the trigger*/

 update Employee_t set TerminationDate= '2024-04-30' where EmployeeID= 11049;

/* Table after update*/

select * from EmployeeAudit_t; -- output - 1	11049	Termination	2024-04-30 15:07:50		2024-04-30

