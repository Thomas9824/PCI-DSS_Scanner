# PCI DSS Scanner

Automated scanner for PCI DSS compliance documents and SAQ (Self-Assessment Questionnaire) forms with multi-language support.

## Features

- **Automatic Change Detection**: Monitors PCI Security Standards Council website for document updates
- **Multi-Language Support**: Downloads and extracts both English and French versions of documents
- **PDF Download**: Automated download of PCI DSS standards and SAQ forms
- **Requirements Extraction**: Extracts structured requirements data from PDF documents
- **Email Notifications**: Sends automated email reports with extracted CSV files
- **Intelligent Matching**: Precise document version matching and change detection

## Project Structure

```
pci_scraper/
├── pci_auto_scraper.py          # Main orchestrator script
├── pci_change_scraper/          # Change detection module
├── pci_pdf_scraper/             # PDF download module  
├── pci_pdf_extractor/           # Requirements extraction module
├── requirements.txt             # Python dependencies
├── run.sh                       # Execution script
└── README.md                    # This file
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Thomas9824/PCI-DSS_Scanner.git
cd PCI-DSS_Scanner
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your actual API keys
# Get your Resend API key from: https://resend.com/
```

4. Update `.env` file with your credentials:
```bash
RESEND_API_KEY=your_resend_api_key_here
EMAIL_RECIPIENT=your-email@example.com
```

## Usage

### Quick Start
```bash
python pci_auto_scraper.py
```

### Manual Execution
```bash
chmod +x run.sh
./run.sh
```

## Supported Documents

- PCI DSS v4.0.1 Standard
- SAQ (Self-Assessment Questionnaire) forms:
  - SAQ A, A-EP, B, B-IP, C, C-VT
  - SAQ D (Merchant & Service Provider)  
  - SAQ P2PE, SPOC
- AOC (Attestation of Compliance) forms
- Related guidance documents

## Language Support

The scanner automatically detects and downloads documents in:
- 🇬🇧 **English** - Uses `testv5_EN.py` extractor
- 🇫🇷 **French** - Uses `testv5.py` extractor

## Output

- **CSV Files**: Structured requirements data extracted from PDFs
- **Email Reports**: HTML-formatted reports with download links
- **Change Logs**: Detailed change detection reports
- **PDF Archive**: Downloaded documents organized by session

## Configuration

Key configuration options in `pci_auto_scraper.py`:
- Email recipient address
- Download directory path
- Headless browser mode
- Extraction parameters

## Dependencies

- Python 3.7+
- Selenium WebDriver
- BeautifulSoup4
- PyPDF2
- Pandas
- Requests
- Resend (for email)

## License

This project is for educational and compliance assistance purposes. 
