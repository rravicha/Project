-- phpMyAdmin SQL Dump
-- version 4.8.3
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Sep 08, 2018 at 10:01 PM
-- Server version: 10.1.35-MariaDB
-- PHP Version: 7.2.9

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `payroll`
--

-- --------------------------------------------------------

--
-- Table structure for table `bill`
--

CREATE TABLE `bill` (
  `billsno` int(10) NOT NULL,
  `bill_id` int(10) NOT NULL,
  `company_id` int(10) NOT NULL,
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `bill`
--

INSERT INTO `bill` (`billsno`, `bill_id`, `company_id`, `delete_status`) VALUES
(1, 1, 5, '0'),
(2, 123, 2, '0');

-- --------------------------------------------------------

--
-- Table structure for table `billreceived`
--

CREATE TABLE `billreceived` (
  `bsno` int(10) NOT NULL,
  `billsno` int(10) NOT NULL,
  `totoal_bill` int(10) NOT NULL,
  `received_date` varchar(10) NOT NULL,
  `received_amount` int(11) NOT NULL,
  `reduced` int(10) NOT NULL,
  `remark` varchar(10) NOT NULL,
  `pay_status` enum('0','1','2') NOT NULL DEFAULT '0',
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `bkash`
--

CREATE TABLE `bkash` (
  `bk_id` int(11) NOT NULL,
  `given_date` date NOT NULL DEFAULT '0000-00-00',
  `amount` int(11) NOT NULL DEFAULT '0',
  `remark` varchar(20) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `bkash`
--

INSERT INTO `bkash` (`bk_id`, `given_date`, `amount`, `remark`) VALUES
(4, '2018-08-16', 235000, 'bKash'),
(5, '2018-09-09', 60000, ''),
(6, '2018-09-09', 100000, ''),
(7, '2018-09-09', 100000, ''),
(8, '2018-09-09', 600000, '');

-- --------------------------------------------------------

--
-- Table structure for table `cash`
--

CREATE TABLE `cash` (
  `c_id` int(10) NOT NULL,
  `given_date` date NOT NULL DEFAULT '0000-00-00',
  `amount` int(10) NOT NULL,
  `remark` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `cash`
--

INSERT INTO `cash` (`c_id`, `given_date`, `amount`, `remark`) VALUES
(1, '2018-08-16', 1563, 'Previous Cash'),
(2, '2018-08-16', 10000, 'Masbah Vai Transfer'),
(3, '2018-08-16', 600000, 'Masbah Vai Cheque'),
(4, '2018-08-16', 175000, 'Masbah Vai Cheque'),
(5, '2018-08-16', 300, 'Previous Cash');

-- --------------------------------------------------------

--
-- Table structure for table `company`
--

CREATE TABLE `company` (
  `id` int(10) NOT NULL,
  `name` varchar(30) NOT NULL,
  `company_type` varchar(20) DEFAULT 'Regular',
  `address` varchar(50) NOT NULL,
  `mobile` varchar(20) NOT NULL,
  `delete_status` enum('0','1') NOT NULL DEFAULT '0',
  `details` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `company`
--

INSERT INTO `company` (`id`, `name`, `company_type`, `address`, `mobile`, `delete_status`, `details`) VALUES
(1, 'Tanha Sign', 'Regular', 'Dhaka', '01729561212', '0', '3rd Party'),
(2, 'mArt', 'Regular', 'Mymensingh', '01732089676', '0', '3rd Party'),
(3, 'Salam Art', 'Casual', 'Mymensingh', '01776654678', '0', '3rd Party'),
(4, 'Rofik Art', 'Casual', 'Sirajganj', '01748184964', '0', '3rd Party'),
(5, 'Tutul Art', 'Casual', 'Dhaka', '01756063619', '0', '3rd Party');

-- --------------------------------------------------------

--
-- Table structure for table `companybill`
--

CREATE TABLE `companybill` (
  `s_id` int(100) NOT NULL,
  `bill_no` int(100) NOT NULL DEFAULT '0',
  `billsno` varchar(10) DEFAULT NULL,
  `company_group_id` int(10) NOT NULL,
  `work_type` varchar(50) DEFAULT NULL,
  `work_area` varchar(50) DEFAULT NULL,
  `square_fit` int(11) DEFAULT '0',
  `rate` int(11) DEFAULT '0',
  `bill_date` date NOT NULL DEFAULT '0000-00-00',
  `receive_date` date NOT NULL DEFAULT '0000-00-00',
  `bill_amount` int(11) NOT NULL DEFAULT '0',
  `receive_amount` int(11) NOT NULL DEFAULT '0',
  `reduced` int(11) NOT NULL DEFAULT '0',
  `remark` varchar(50) DEFAULT NULL,
  `pay_status` enum('0','1','2') DEFAULT '0',
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `companybill`
--

INSERT INTO `companybill` (`s_id`, `bill_no`, `billsno`, `company_group_id`, `work_type`, `work_area`, `square_fit`, `rate`, `bill_date`, `receive_date`, `bill_amount`, `receive_amount`, `reduced`, `remark`, `pay_status`, `delete_status`) VALUES
(1, 121, '2', 1, 'Paint', 'Rupnagar', 1200, 13, '2018-09-09', '2018-09-09', 15600, 10000, 10600, '', '2', '0');

-- --------------------------------------------------------

--
-- Table structure for table `compaygroup`
--

CREATE TABLE `compaygroup` (
  `s_id` int(10) NOT NULL,
  `id` int(10) NOT NULL,
  `g_id` int(11) NOT NULL,
  `g_name` varchar(100) NOT NULL,
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `compaygroup`
--

INSERT INTO `compaygroup` (`s_id`, `id`, `g_id`, `g_name`, `delete_status`) VALUES
(1, 2, 1, 'Mrt-1', '0');

-- --------------------------------------------------------

--
-- Table structure for table `deductions`
--

CREATE TABLE `deductions` (
  `deduction_id` int(5) NOT NULL,
  `emp_id` int(10) NOT NULL,
  `d_date` varchar(50) NOT NULL,
  `d_cause` varchar(50) NOT NULL,
  `d_amount` int(10) NOT NULL,
  `d_method` set('cash','bkash') NOT NULL DEFAULT 'cash',
  `remark` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `deductions`
--

INSERT INTO `deductions` (`deduction_id`, `emp_id`, `d_date`, `d_cause`, `d_amount`, `d_method`, `remark`) VALUES
(130, 1, '09-09-2018', 'payment', 3000, 'bkash', ''),
(131, 11, '09-09-2018', 'payment', 5000, 'bkash', '');

-- --------------------------------------------------------

--
-- Table structure for table `employee`
--

CREATE TABLE `employee` (
  `emp_id` int(10) NOT NULL,
  `lname` varchar(20) NOT NULL,
  `fname` varchar(20) NOT NULL,
  `gender` varchar(6) NOT NULL,
  `emp_type` varchar(20) NOT NULL,
  `division` varchar(30) NOT NULL,
  `mobileNo` varchar(20) DEFAULT NULL,
  `salary` int(10) NOT NULL DEFAULT '0',
  `bonus` int(10) NOT NULL DEFAULT '0',
  `loan` int(11) NOT NULL DEFAULT '0',
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `employee`
--

INSERT INTO `employee` (`emp_id`, `lname`, `fname`, `gender`, `emp_type`, `division`, `mobileNo`, `salary`, `bonus`, `loan`, `delete_status`) VALUES
(1, 'Zaman', 'Ohid', 'Male', 'Regular', 'Control', '01727585433', 24000, 0, 0, '0'),
(2, 'Islam', 'Tariqul', 'Male', 'Regular', 'Control', '01736274402', 13000, 0, 0, '0'),
(3, '(Emon)', 'Abu Bakar Siddik', 'Male', 'Regular', 'Accounting', '01616882595', 12000, 0, 0, '0'),
(4, '(Prince)', 'Shahidul Islam', 'Male', 'Regular', 'Human Resource', '01714891409', 15000, 0, 0, '0'),
(5, 'Babu', 'Abu Helal', 'Male', 'Regular', 'Maintenance', '01728195996', 8000, 0, 0, '0'),
(6, 'Islam', 'Md Nazrul', 'Male', 'Regular', 'Maintenance', '01727918716', 10000, 0, 0, '0'),
(7, 'Uddin', 'Md Johir', 'Male', 'Regular', 'Control', '01749405020', 10500, 0, 0, '0'),
(8, 'Mia', 'Shahadat', 'Male', 'Regular', 'Control', '01640951295', 8000, 0, 0, '0'),
(9, ' ', 'Rajon', 'Male', 'Regular', 'Control', '01914267700', 8000, 0, 0, '0'),
(10, ' ', 'Yeakub', 'Male', 'Regular', 'Control', '01', 4000, 0, 0, '0'),
(11, 'Mobarok', 'Md', 'Male', 'Casual', 'Control', '01746933666', 550, 700, 0, '0'),
(12, 'Uddin', 'Jalal', 'Male', 'Casual', 'Control', '01721228674', 530, 300, 0, '0'),
(13, 'Rizu', 'Yeadul Islam', 'Male', 'Casual', 'Control', '01719255610', 520, 700, 0, '0'),
(14, 'Zaman', 'Moniruz', 'Male', 'Casual', 'Control', '01729508266', 500, 0, 0, '0'),
(15, 'Khair', 'Abul', 'Male', 'Casual', 'Control', '01738626468', 450, 700, 0, '0'),
(16, 'Ahmed', 'Forid', 'Male', 'Casual', 'Control', '01795989312', 520, 300, 0, '0'),
(17, ' ', 'Mehrul', 'Male', 'Casual', 'Control', '01720605029', 350, 500, 0, '0'),
(18, 'Teli', 'Jashim', 'Male', 'Regular', 'Control', '01775895956', 8000, 0, 0, '0'),
(19, ' ', 'Ajmul', 'Male', 'Regular', 'Control', '0', 4000, 0, 0, '0'),
(20, 'Art', 'Johir', 'Male', 'Casual', 'Control', '01838474791', 450, 1200, 0, '0'),
(21, 'Hossen', 'Akbar ', 'Male', 'Job Order', 'Supply', '01729561212', 500, 0, 0, '0'),
(22, 'Islam', 'Majharul', 'Male', 'Job Order', 'Supply', '01732089676', 0, 0, 0, '0'),
(23, 'Hossen', 'Saddam', 'Male', 'Job Order', 'Supply', '01776654678', 0, 0, 0, '0'),
(24, 'Islam', 'Tutul', 'Male', 'Job Order', 'Supply', '01756063619', 0, 0, 0, '0');

-- --------------------------------------------------------

--
-- Table structure for table `overtime`
--

CREATE TABLE `overtime` (
  `ot_id` int(10) NOT NULL,
  `rate` int(10) NOT NULL DEFAULT '0',
  `emp_id` int(10) NOT NULL,
  `ot_date` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `payment`
--

CREATE TABLE `payment` (
  `pay_id` int(10) NOT NULL,
  `emp_id` int(10) NOT NULL,
  `pay_date` date NOT NULL DEFAULT '0000-00-00',
  `pay_amount` int(10) NOT NULL DEFAULT '0',
  `paid_in_cash` int(10) NOT NULL DEFAULT '0',
  `paid_in_bkash` int(10) NOT NULL DEFAULT '0',
  `due` int(10) NOT NULL DEFAULT '0',
  `due_status` enum('0','1') NOT NULL DEFAULT '0',
  `advance` int(10) NOT NULL DEFAULT '0',
  `advance_status` enum('0','1') NOT NULL DEFAULT '0',
  `pay_remark` varchar(100) DEFAULT NULL,
  `pay_status` enum('0','1') NOT NULL DEFAULT '0',
  `delete_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `product`
--

CREATE TABLE `product` (
  `p_id` int(10) NOT NULL,
  `p_name` varchar(50) NOT NULL,
  `p_company` varchar(100) NOT NULL,
  `p_type` varchar(50) NOT NULL,
  `p_quantity` varchar(50) NOT NULL,
  `price` int(10) NOT NULL,
  `stock` int(10) NOT NULL DEFAULT '0',
  `delete_status` enum('0','1') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `product`
--

INSERT INTO `product` (`p_id`, `p_name`, `p_company`, `p_type`, `p_quantity`, `price`, `stock`, `delete_status`) VALUES
(2, 'White', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 12, '0'),
(3, 'White', 'RAK Paints', 'Enamel-B', '3.64 Ltr', 950, 27, '0'),
(4, 'White', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(5, 'White', 'Elite Paint', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(6, 'White', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(7, 'White', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(8, 'White', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 950, 0, '0'),
(9, 'White', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(10, 'White', 'Rainbow Paints', 'Enamel-B', '3.64 Ltr', 950, 10, '0'),
(11, 'White', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(12, 'Yellow', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(13, 'Yellow', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 18, '0'),
(14, 'Yellow', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(15, 'Yellow', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(16, 'Yellow', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(17, 'Yellow', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(18, 'Yellow', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(19, 'Yellow', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(20, 'Yellow', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(21, 'Yellow', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(22, 'Orange', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 1, '0'),
(23, 'Orange', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(24, 'Orange', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(25, 'Orange', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(26, 'Orange', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(27, 'Orange', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(28, 'Orange', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(29, 'Orange', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(30, 'Orange', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(31, 'Orange', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(32, 'Black', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 5, '0'),
(33, 'Black', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(34, 'Black', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 1, '0'),
(35, 'Black', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(36, 'Black', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(37, 'Black', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(38, 'Black', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(39, 'Black', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(40, 'Black', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(41, 'Black', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(42, 'G.Green', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 11, '0'),
(43, 'G.Green', 'RAK Paints', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(44, 'G.Green', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(45, 'G.Green', 'Elite Paint', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(46, 'G.Green', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(47, 'G.Green', 'Roxy Paint', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(48, 'G.Green', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 1, '0'),
(49, 'G.Green', 'Roxy Paint', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(50, 'G.Green', 'Nippon Paint', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(51, 'G.Green', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(52, 'G.Green', 'Rainbow Paints', 'Enamel-B', '3.64 Ltr', 950, 0, '0'),
(53, 'Red', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 5, '0'),
(54, 'Red', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(55, 'Red', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(56, 'Red', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(57, 'Red', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(58, 'Red', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(59, 'Red', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(60, 'Red', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(61, 'Red', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(62, 'Red', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(63, 'Red', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(64, 'Calemon ', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 15, '0'),
(65, 'Light blue', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 5, '0'),
(66, 'Calemon ', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 27, '0'),
(67, 'Calemon ', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 3, '0'),
(68, 'Calemon ', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(69, 'Calemon ', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(70, 'Calemon ', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(71, 'Calemon ', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 2, '0'),
(72, 'Calemon ', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(73, 'Calemon ', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(74, 'Calemon ', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(75, 'Navy blue', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 5, '0'),
(76, 'Navy blue', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(77, 'Navy blue', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(78, 'Navy blue', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(79, 'Navy blue', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(80, 'Navy blue', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(81, 'Navy blue', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(82, 'Navy blue', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(89, 'Navy blue', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(90, 'Navy blue', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(91, 'Light blue', 'RAK Paints', 'Enamel B', '3.64 Ltr', 950, 18, '0'),
(92, 'Light blue', 'Elite Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(93, 'Light blue', 'Elite Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(94, 'Light blue', 'Roxy Paint', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(95, 'Light blue', 'Roxy Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(96, 'Light blue', 'Nippon Paint', 'Enamel-A', '3.64 Ltr', 1200, 2, '0'),
(97, 'Light blue', 'Nippon Paint', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(98, 'Light blue', 'Rainbow Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0'),
(99, 'Light blue', 'Rainbow Paints', 'Enamel B', '3.64 Ltr', 950, 0, '0'),
(100, 'White', 'RAK Paints', 'Plastic Paint', '18.2 Ltr', 7500, 4, '0'),
(101, 'White', 'Nippon Paint', 'Weather Coat', '18.2 Ltr', 3500, 3, '0'),
(102, 'White', 'Elite Paint', 'Weather Coat', '3.64 Ltr', 1200, 7, '0'),
(103, 'White', 'Elite Paint', 'Mustard coat', '3.64 Ltr', 1200, 2, '0'),
(104, 'Black', 'RAK Paints', 'Weather Coat', '3.64 Ltr', 1200, 5, '0'),
(105, 'Umbrella', 'N/A', '16 Shift', 'Pcs', 150, 2, '0'),
(106, 'Orange', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 6, '0'),
(107, 'Yellow', 'RAK Paints', 'Enamel-A', '3.64 Ltr', 1200, 0, '0');

-- --------------------------------------------------------

--
-- Table structure for table `product_operation_history`
--

CREATE TABLE `product_operation_history` (
  `sno` int(11) NOT NULL,
  `pid` int(11) NOT NULL,
  `quantity` int(11) NOT NULL,
  `operation` varchar(20) NOT NULL,
  `date` date NOT NULL DEFAULT '2018-00-00',
  `value` int(11) NOT NULL,
  `result` int(11) NOT NULL,
  `remark` varchar(50) NOT NULL,
  `edit_status` enum('0','1') NOT NULL DEFAULT '0'
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `product_operation_history`
--

INSERT INTO `product_operation_history` (`sno`, `pid`, `quantity`, `operation`, `date`, `value`, `result`, `remark`, `edit_status`) VALUES
(42, 3, 40, 'sub', '2018-08-18', 1, 39, 'Majharul vai', '0'),
(41, 4, 40, 'sub', '2018-08-18', 40, 0, 'Adjust', '0'),
(39, 3, 0, 'add', '2018-08-16', 42, 42, 'RAK Paints', '0'),
(40, 3, 42, 'sub', '2018-08-16', 2, 40, 'Rofik', '0'),
(37, 2, 40, 'add', '2018-08-16', 2, 42, 'RAK Paints', '0'),
(38, 2, 42, 'sub', '2018-08-16', 42, 0, 'For Correction', '1'),
(43, 48, 5, 'sub', '2018-08-18', 2, 3, 'Majharul vai', '0'),
(44, 48, 3, 'sub', '2018-08-19', 2, 1, 'Majharul vai', '0'),
(45, 22, 3, 'sub', '2018-08-29', 2, 1, 'Kamrul Vai', '0'),
(46, 28, 4, 'sub', '2018-08-29', 2, 2, 'Kamrul', '0'),
(47, 22, 1, 'sub', '2018-08-29', 0, 1, 'Tutul vai', '1'),
(48, 28, 2, 'sub', '2018-08-29', 2, 0, 'Tutul vai', '1'),
(49, 13, 40, 'sub', '2018-08-29', 2, 38, 'Tutul vai', '0'),
(50, 3, 39, 'sub', '2018-08-29', 3, 36, 'Dhaka site', '1'),
(51, 65, 7, 'sub', '2018-08-29', 2, 5, 'Dhaka site', '0'),
(52, 91, 27, 'sub', '2018-08-30', 3, 24, 'Dhaka site', '0'),
(53, 42, 1, 'add', '2018-09-01', 10, 11, 'Received by Emon', '0'),
(54, 32, 2, 'add', '2018-09-01', 5, 7, 'Received by Emon', '0'),
(55, 2, 0, 'add', '2018-09-01', 20, 20, 'Received by Emon', '0'),
(56, 105, 0, 'add', '2018-09-01', 50, 50, 'New Purchase', '0'),
(57, 13, 38, 'sub', '2018-08-31', 10, 28, 'Akbar', '0'),
(58, 3, 36, 'sub', '2018-08-31', 5, 31, 'Akbar', '0'),
(59, 32, 7, 'sub', '2018-08-31', 2, 5, 'Akbar', '0'),
(60, 2, 20, 'sub', '2018-09-03', 2, 18, 'Saddam', '0'),
(61, 3, 31, 'sub', '2018-09-03', 1, 30, 'Saddam', '0'),
(62, 13, 28, 'sub', '2018-09-03', 2, 26, 'Saddam', '0'),
(63, 107, 1, 'sub', '2018-09-03', 1, 0, 'Saddam', '0'),
(64, 105, 50, 'sub', '2018-09-03', 2, 48, 'School Van', '0'),
(65, 105, 48, 'sub', '2018-09-03', 40, 8, 'Majharul vai, Volkanising', '0'),
(66, 105, 8, 'sub', '2018-09-03', 1, 7, 'Nazu vai', '0'),
(67, 105, 7, 'sub', '2018-09-03', 5, 2, 'Tutul vai', '0'),
(68, 106, 10, 'sub', '2018-09-03', 2, 8, 'Tutul vai', '0'),
(69, 13, 26, 'sub', '2018-09-03', 2, 24, 'Tutul vai', '0'),
(70, 2, 18, 'sub', '2018-09-04', 2, 16, 'Rofik', '0'),
(71, 3, 30, 'sub', '2018-09-04', 2, 28, 'Rofik', '0'),
(72, 13, 24, 'sub', '2018-09-04', 2, 22, 'Rofik', '0'),
(73, 106, 8, 'sub', '2018-09-04', 2, 6, 'Rofik', '0'),
(74, 2, 16, 'sub', '2018-09-04', 1, 15, 'Saddam', '0'),
(75, 3, 28, 'sub', '2018-09-04', 1, 27, 'Saddam', '0'),
(76, 13, 22, 'sub', '2018-09-04', 2, 20, 'Saddam', '0'),
(77, 91, 24, 'sub', '2018-09-04', 9, 15, 'Nazu vai', '0'),
(78, 91, 15, 'add', '2018-09-04', 3, 18, 'ferot', '0'),
(79, 92, 3, 'sub', '2018-09-04', 3, 0, 'Dhaka site- nazu vai', '0'),
(80, 2, 15, 'sub', '2018-09-04', 3, 12, 'Dhaka site- nazu vai', '0'),
(81, 13, 20, 'sub', '2018-09-06', 2, 18, 'Dhaka site- nazu vai', '0'),
(82, 10, 0, 'add', '2018-09-09', 10, 10, '', '0');

-- --------------------------------------------------------

--
-- Table structure for table `receivedbill`
--

CREATE TABLE `receivedbill` (
  `sno` int(11) NOT NULL,
  `s_id` int(11) NOT NULL,
  `bill_date` date NOT NULL DEFAULT '2018-00-00',
  `amount` int(11) NOT NULL DEFAULT '0',
  `remark` varchar(40) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `receivedbill`
--

INSERT INTO `receivedbill` (`sno`, `s_id`, `bill_date`, `amount`, `remark`) VALUES
(57, 1, '2018-09-09', 5000, ''),
(58, 1, '2018-09-09', 5000, '');

-- --------------------------------------------------------

--
-- Table structure for table `reset`
--

CREATE TABLE `reset` (
  `email` varchar(20) NOT NULL,
  `password` int(20) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `salary`
--

CREATE TABLE `salary` (
  `salary_id` int(10) NOT NULL,
  `emp_id` int(10) NOT NULL,
  `salary_rate` int(10) NOT NULL,
  `bonus` int(10) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `stock`
--

CREATE TABLE `stock` (
  `s_id` int(10) NOT NULL,
  `p_id` int(10) NOT NULL,
  `stock_quantity` int(10) NOT NULL DEFAULT '0',
  `status` varchar(10) NOT NULL,
  `remark` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `transaction`
--

CREATE TABLE `transaction` (
  `sno` int(10) NOT NULL,
  `t_date` date NOT NULL,
  `amount` int(10) NOT NULL,
  `cause` varchar(50) NOT NULL,
  `method` set('cash','bkash') NOT NULL DEFAULT 'cash',
  `remark` varchar(100) DEFAULT NULL,
  `delete_status` enum('0','1') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `transaction`
--

INSERT INTO `transaction` (`sno`, `t_date`, `amount`, `cause`, `method`, `remark`, `delete_status`) VALUES
(59, '2018-08-16', 500, 'Artist', 'cash', 'Akbar', '0'),
(60, '2018-08-16', 500, 'Masbah', 'cash', 'Site Khoroch', '0'),
(61, '2018-08-16', 730, 'Dhaka Site', 'cash', 'Jalal', '0'),
(62, '2018-08-16', 350, 'Prince', 'cash', 'Personal', '0'),
(63, '2018-08-16', 25, 'Courier Bill', 'cash', '', '0'),
(64, '2018-08-16', 200, 'Tarik Vai', 'cash', 'Khoroch', '0'),
(65, '2018-08-16', 235000, 'Feroz bKash', 'cash', '', '0'),
(66, '2018-08-16', 139155, 'RFL Distributor', 'cash', 'For Biswas Trading', '0'),
(67, '2018-08-16', 25000, 'Bhola Glass', 'cash', 'Board', '0'),
(68, '2018-08-16', 1000, 'Helal', 'cash', 'Personal', '0'),
(69, '2018-08-16', 42230, 'Malamal', 'cash', '', '0'),
(70, '2018-08-16', 2000, 'Mal Pathano', 'cash', 'Saiful', '0'),
(71, '2018-08-16', 2000, 'Saiful', 'cash', 'Mal Pathano', '0'),
(72, '2018-08-16', 7800, 'Classical Trade', 'cash', 'Electric Malamal', '0'),
(73, '2018-08-16', 670, 'Emon', 'cash', 'Khoroch', '0'),
(74, '2018-08-16', 3664, 'Pipe', 'cash', 'Rajon', '0'),
(75, '2018-08-16', 200, 'Moyla Bil', 'cash', 'House', '0'),
(76, '2018-08-16', 100, 'Mobile Bill', 'cash', 'Office', '0'),
(77, '2018-08-16', 320, 'Dhaka Site', 'cash', 'Forid', '0'),
(78, '2018-08-16', 838440, 'Previous Balance', 'bkash', '', '0'),
(79, '2018-08-16', 3000, 'Akbar', 'bkash', '', '0'),
(80, '2018-08-16', 5000, 'Shahalom', 'bkash', '', '0'),
(81, '2018-08-16', 8000, 'Saiful', 'bkash', 'Angal/Pipe', '0'),
(82, '2018-08-16', 2000, 'Cox-Bazar Site', 'bkash', 'Babu', '0'),
(83, '2018-08-16', 2000, 'Rofik', 'bkash', '', '0'),
(84, '2018-08-16', 1500, 'Kamrul', 'bkash', '', '0'),
(85, '2018-08-16', 5000, 'Majharul', 'bkash', '', '0'),
(86, '2018-08-16', 2000, 'Tutul', 'bkash', '', '0'),
(87, '2018-08-16', 2000, 'Salam', 'bkash', '', '0'),
(88, '2018-08-16', 1500, 'Akbar', 'bkash', '', '0'),
(89, '2018-08-16', 2000, 'Saiful', 'bkash', '', '0');

-- --------------------------------------------------------

--
-- Table structure for table `user`
--

CREATE TABLE `user` (
  `id` int(5) NOT NULL,
  `username` varchar(20) NOT NULL,
  `password` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `user`
--

INSERT INTO `user` (`id`, `username`, `password`) VALUES
(2, 'admin', '5f4dcc3b5aa765d61d8327deb882cf99'),
(3, 'anupam', 'ae35b1c2f8041905894d93968ba73773'),
(5, 'Abirahmed', '76274ce87cd4374bd620ef3e0eff5fac');

-- --------------------------------------------------------

--
-- Table structure for table `works`
--

CREATE TABLE `works` (
  `w_id` int(10) NOT NULL,
  `emp_id` int(11) NOT NULL,
  `w_date` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `works`
--

INSERT INTO `works` (`w_id`, `emp_id`, `w_date`) VALUES
(1, 11, 20),
(2, 12, 15),
(3, 13, 16),
(4, 14, 0),
(5, 15, 14),
(6, 16, 15),
(7, 17, 13),
(8, 20, 2);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `bill`
--
ALTER TABLE `bill`
  ADD PRIMARY KEY (`billsno`);

--
-- Indexes for table `billreceived`
--
ALTER TABLE `billreceived`
  ADD PRIMARY KEY (`bsno`);

--
-- Indexes for table `bkash`
--
ALTER TABLE `bkash`
  ADD PRIMARY KEY (`bk_id`);

--
-- Indexes for table `cash`
--
ALTER TABLE `cash`
  ADD PRIMARY KEY (`c_id`);

--
-- Indexes for table `company`
--
ALTER TABLE `company`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `companybill`
--
ALTER TABLE `companybill`
  ADD PRIMARY KEY (`s_id`);

--
-- Indexes for table `compaygroup`
--
ALTER TABLE `compaygroup`
  ADD PRIMARY KEY (`s_id`);

--
-- Indexes for table `deductions`
--
ALTER TABLE `deductions`
  ADD PRIMARY KEY (`deduction_id`);

--
-- Indexes for table `employee`
--
ALTER TABLE `employee`
  ADD PRIMARY KEY (`emp_id`);

--
-- Indexes for table `overtime`
--
ALTER TABLE `overtime`
  ADD PRIMARY KEY (`ot_id`);

--
-- Indexes for table `payment`
--
ALTER TABLE `payment`
  ADD PRIMARY KEY (`pay_id`);

--
-- Indexes for table `product`
--
ALTER TABLE `product`
  ADD PRIMARY KEY (`p_id`);

--
-- Indexes for table `product_operation_history`
--
ALTER TABLE `product_operation_history`
  ADD PRIMARY KEY (`sno`);

--
-- Indexes for table `receivedbill`
--
ALTER TABLE `receivedbill`
  ADD PRIMARY KEY (`sno`);

--
-- Indexes for table `salary`
--
ALTER TABLE `salary`
  ADD PRIMARY KEY (`salary_id`);

--
-- Indexes for table `stock`
--
ALTER TABLE `stock`
  ADD PRIMARY KEY (`s_id`);

--
-- Indexes for table `transaction`
--
ALTER TABLE `transaction`
  ADD PRIMARY KEY (`sno`);

--
-- Indexes for table `user`
--
ALTER TABLE `user`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `works`
--
ALTER TABLE `works`
  ADD PRIMARY KEY (`w_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `bill`
--
ALTER TABLE `bill`
  MODIFY `billsno` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `billreceived`
--
ALTER TABLE `billreceived`
  MODIFY `bsno` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `bkash`
--
ALTER TABLE `bkash`
  MODIFY `bk_id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=9;

--
-- AUTO_INCREMENT for table `cash`
--
ALTER TABLE `cash`
  MODIFY `c_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;

--
-- AUTO_INCREMENT for table `company`
--
ALTER TABLE `company`
  MODIFY `id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;

--
-- AUTO_INCREMENT for table `companybill`
--
ALTER TABLE `companybill`
  MODIFY `s_id` int(100) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `compaygroup`
--
ALTER TABLE `compaygroup`
  MODIFY `s_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `deductions`
--
ALTER TABLE `deductions`
  MODIFY `deduction_id` int(5) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=132;

--
-- AUTO_INCREMENT for table `employee`
--
ALTER TABLE `employee`
  MODIFY `emp_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=25;

--
-- AUTO_INCREMENT for table `overtime`
--
ALTER TABLE `overtime`
  MODIFY `ot_id` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `payment`
--
ALTER TABLE `payment`
  MODIFY `pay_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `product`
--
ALTER TABLE `product`
  MODIFY `p_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=108;

--
-- AUTO_INCREMENT for table `product_operation_history`
--
ALTER TABLE `product_operation_history`
  MODIFY `sno` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=83;

--
-- AUTO_INCREMENT for table `receivedbill`
--
ALTER TABLE `receivedbill`
  MODIFY `sno` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=59;

--
-- AUTO_INCREMENT for table `salary`
--
ALTER TABLE `salary`
  MODIFY `salary_id` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `stock`
--
ALTER TABLE `stock`
  MODIFY `s_id` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `transaction`
--
ALTER TABLE `transaction`
  MODIFY `sno` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=90;

--
-- AUTO_INCREMENT for table `user`
--
ALTER TABLE `user`
  MODIFY `id` int(5) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;

--
-- AUTO_INCREMENT for table `works`
--
ALTER TABLE `works`
  MODIFY `w_id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=9;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
