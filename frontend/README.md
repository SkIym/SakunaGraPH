# Sakuna.PH — User Manual

> A web-based disaster data visualization platform for the Philippines.

---

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Developing](#developing)
  - [Building](#building)
- [Navigating the Website](#navigating-the-website)
  - [Home](#home)
  - [About](#about)
  - [Disasters](#disasters)
  - [Account](#account)
- [Features](#features)
  - [Data Filters](#data-filters)
  - [Dark Mode](#dark-mode)
  - [Download Data](#download-data)
- [Notes & Disclaimers](#notes--disclaimers)

---

## Overview

**Sakuna.PH** is a disaster data visualization web application focused on the Philippines. It provides multiple ways to explore and analyze disaster records — through charts, maps, timelines, and tables — with filtering tools to narrow data by location, disaster type, and date range.

> ⚠️ **This project is currently under active development.** Disaster records may be fictional or incomplete. Do not share personal information on this website.

---

## Getting Started

### Developing

Install dependencies first:

```sh
npm install
# or
pnpm install
# or
yarn
```

Then start the development server:

```sh
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

### Building

To create a production version of the app:

```sh
npm run build
```

Preview the production build locally:

```sh
npm run preview
```

---

## Navigating the Website

### Home

The landing page provides a short introduction to Sakuna.PH — its purpose and scope. Use the navigation bar at the top to move between sections of the site.

---

### About

The About page is divided into two sections:

**Sakuna PH**
A background of the project including its mission, goals, and context within Philippine disaster preparedness and data accessibility.

**Team**
A directory of the people behind the project, organized by role:
- **Developers** — built and maintain the platform
- **Managers** — oversee project database and integration
- **Contributors** — provided data, research, or domain expertise

---

### Disasters

The Disasters section is the core of the platform. It offers four different views of the same underlying disaster dataset, each accessible from the navigation menu.

#### Metric View
Displays high-level summary statistics for the filtered dataset:
- **Toll Count** — total number of casualties
- **Economic Damage** — total monetary damage in Philippine Pesos (compact format)
- **Affected Families** — total number of families affected

These numbers update automatically when filters are applied.

#### Map View
An interactive map showing disaster occurrences geographically across the Philippines. Use this view to identify regional patterns and hotspots.

#### Timeline View
A chronological visualization of disasters over time. Useful for identifying seasonal trends or periods of high disaster frequency.

#### Table View
A sortable, selectable tabular view of all disaster records found in the database.

On mobile, the table scrolls horizontally to accommodate all columns.

Signed-in users can **download the full dataset** as a CSV file using the Download button above the table.

---

### Account

#### Signing In
Click **Sign in with Google** in the top-right corner of the navigation bar. Authentication is handled entirely through your Google account — no separate password is required.

#### Profile Setup
First-time users will be prompted to complete a profile form after signing in. This includes:
- Display name
- Birthdate
- Occupation
- Address
- Affiliation (university or organization)
- Intended usage of the platform

#### Editing Your Profile
After setup, signed-in users can update their profile at any time by clicking their name/avatar in the navigation bar and selecting **Edit Profile**.

#### Signing Out
Click your name/avatar in the navigation bar and select **Logout**.

---

## Features

### Data Filters

A filter panel is available on all Disaster pages. It allows you to narrow the dataset by:

- **Location** — select one or more regions, provinces, or cities/municipalities. Supports search and a "My Location" button that auto-selects your current area.
- **Disaster Type** — select one or more disaster categories or subtypes from a hierarchical list.
- **Date Range** — pick a start and end date to restrict records to a specific period.

All views (Metric, Map, Timeline, Table) respond to the active filters simultaneously.

On **desktop**, the filter panel appears as a sidebar on the left side of the page.
On **mobile**, the filter panel is hidden by default. Tap the **Filters** button (bottom-right of the screen) to open it as a bottom sheet. An active filter count badge appears on the button when filters are applied.

---

### Dark Mode

A dark mode toggle is available in the top-right corner of the navigation bar on all pages. Your preference applies across the entire site including charts, cards, and tables.

---

### Download Data

Signed-in users can export the currently filtered dataset as a `.csv` file from the **Table** view. The Download button is disabled for users who are not signed in.

---

## Notes & Disclaimers

- Disaster records are currently sourced from demo/fictional data and **should not be used for real decision-making**.
- Some features may be incomplete or subject to change during active development.
- Authentication is provided via Google OAuth. No passwords are stored by this application.
- Profile information submitted through the platform is stored securely and used only for platform personalization.