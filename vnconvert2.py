import csv
import re
import os
import sys

def vni2unicode(text):
    # Define the lists of Unicode and VNI characters
    uniChars1 = ['Ấ', 'ấ', 'Ầ', 'ầ', 'Ẩ', 'ẩ', 'Ẫ', 'ẫ', 'Ậ', 'ậ', 'Ắ', 'ắ',
                 'Ằ', 'ằ', 'Ẳ', 'ẳ', 'Ẵ', 'ẵ', 'Ặ', 'ặ', 'Ế', 'ế', 'Ề', 'ề', 'Ể', 'ể',
                 'Ễ', 'ễ', 'Ệ', 'ệ', 'Ố', 'ố', 'Ồ', 'ồ', 'Ổ', 'ổ', 'Ỗ', 'ỗ',
                 'Ộ', 'ộ', 'Ớ', 'ớ', 'Ờ', 'ờ', 'Ở', 'ở', 'Ỡ', 'ỡ',
                 'Ợ', 'ợ', 'Ố', 'ố', 'Ồ', 'ồ', 'Ổ', 'ổ', 'Ỗ', 'ỗ',
                 'Ộ', 'ộ', 'Ớ', 'ớ', 'Ờ', 'ờ', 'Ở', 'ở', 'Ỡ', 'ỡ',
                 'Ợ', 'ợ', 'Ứ', 'ứ', 'Ừ', 'ừ',
                 'Ử', 'ử', 'Ữ', 'ữ', 'Ự', 'ự']

    vniChars1 = ['AÁ', 'aá', 'AÀ', 'aà', 'AÅ', 'aå', 'AÃ', 'aã', 'AÄ', 'aä', 'AÉ', 'aé',
                 'AÈ', 'aè', 'AÚ', 'aú', 'AÜ', 'aü', 'AË', 'aë', 'EÁ', 'eá', 'EÀ', 'eà', 'EÅ', 'eå',
                 'EÃ', 'eã', 'EÄ', 'eä', 'OÁ', 'oá', 'OÀ', 'oà', 'OÅ', 'oå', 'OÃ', 'oã',
                 'OÄ', 'oä', 'ÔÙ', 'ôù', 'ÔØ', 'ôø', 'ÔÛ', 'ôû', 'ÔÕ', 'ôõ',
                 'ÔÏ', 'ôï', 'OÁ', 'oá', 'OÀ', 'oà', 'OÅ', 'oå', 'OÃ', 'oã',
                 'OÄ', 'oä', 'ÔÙ', 'ôù', 'ÔØ', 'ôø', 'ÔÛ', 'ôû', 'ÔÕ', 'ôõ',
                 'ÔÏ', 'ôï', 'ÖÙ', 'öù', 'ÖØ', 'öø',
                 'ÖÛ', 'öû', 'ÖÕ', 'öõ', 'ÖÏ', 'öï']

    uniChars = ['Ơ', 'ơ', 'ĩ', 'Ị', 'ị', 'À', 'Á', 'Â', 'Ã', 'È', 'É', 'Ê', 'Ì', 'Í', 'Ò',
                'Ó', 'Ô', 'Õ', 'Ù', 'Ú', 'Ý', 'à', 'á', 'â', 'ã', 'è', 'é', 'ê', 'ì', 'í', 'ò',
                'ó', 'ô', 'õ', 'ù', 'ú', 'ý', 'Ă', 'ă', 'Đ', 'đ', 'Ĩ', 'Ũ', 'ũ',
                'Ư', 'ư', 'Ạ', 'ạ', 'Ả', 'ả', 'Ẹ', 'ẹ', 'Ẻ', 'ẻ', 'Ẽ', 'ẽ', 'Ỉ', 'ỉ', 'Ọ', 'ọ',
                'Ỏ', 'ỏ', 'Ụ', 'ụ', 'Ủ', 'ủ', 'Ỳ', 'ỳ', 'Ỵ', 'ỵ', 'Ỷ', 'ỷ', 'Ỹ', 'ỹ']

    vniChars = ['Ô', 'ô', 'ó', 'Ò', 'ò', 'AØ', 'AÙ', 'AÂ', 'AÕ', 'EØ', 'EÙ', 'EÂ', 'Ì', 'Í', 'OØ',
                'OÙ', 'OÂ', 'OÕ', 'UØ', 'UÙ', 'YÙ', 'aø', 'aù', 'aâ', 'aõ', 'eø', 'eù', 'eâ', 'ì', 'í',
                'oø', 'où', 'oâ', 'oõ', 'uø', 'uù', 'yù', 'AÊ', 'aê', 'Ñ', 'ñ', 'Ó', 'UÕ', 'uõ',
                'Ö', 'ö', 'AÏ', 'aï', 'AÛ', 'aû', 'EÏ', 'eï', 'EÛ', 'eû', 'EÕ', 'eõ', 'Æ', 'æ', 'OÏ', 'oï',
                'OÛ', 'oû', 'UÏ', 'uï', 'UÛ', 'uû', 'YØ', 'yø', 'Î', 'î', 'YÛ', 'yû', 'YÕ', 'yõ']

    # Replace VNI characters with Unicode characters
    for vni, uni in zip(vniChars1, uniChars1):
        text = re.sub(vni, uni, text)

    for vni, uni in zip(vniChars, uniChars):
        text = re.sub(vni, uni, text)

    return text

def convert_csv_vni_to_unicode(input_csv):
    output_csv = os.path.splitext(input_csv)[0] + '_unicode.csv'
    
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        for row in reader:
            # Convert each cell in the row from VNI to Unicode
            converted_row = [vni2unicode(cell) for cell in row]
            writer.writerow(converted_row)

    print(f"Converted file saved as {output_csv}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python convert_csv_vni_to_unicode.py <input_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    convert_csv_vni_to_unicode(input_csv)
