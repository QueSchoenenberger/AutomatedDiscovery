import os
from PIL import Image
from pytesseract import pytesseract
import cv2


def get_text_from_image(filepath):
    path_to_tesseract = ""
    img = Image.open(filepath)
    cv_img = cv2.imread(filepath)

    if os.name == 'nt':
        path_to_tesseract = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    else:
        path_to_tesseract = "/usr/bin/tesseract"

    pytesseract.tesseract_cmd = path_to_tesseract

    data = pytesseract.image_to_data(img, output_type='dict')

    return " ".join(data['text']), data, cv_img


def mask_image(img, output_path, processed_text, data):
    processed_data = processed_text.split(" ")
    processed_data = [item for item in processed_data if item.strip() != '']
    processed_data = [item for item in processed_data if item.strip() != ' ']

    raw_data = [item for item in data['text'] if item.strip() != '']
    raw_data = [item for item in raw_data if item.strip() != ' ']

    for i in range(len(raw_data)):
        if raw_data[i] in processed_data[i] or processed_data[i] in raw_data[i]:
            pass
        elif "[MASKED]" in processed_data[i]:
            pass
        else:
            processed_data.insert(i - 1, "[MASKED]")

    masked_indexes = []
    sorted_indexes = []
    for index in range(0, len(processed_data)):
        if '[MASKED]' in processed_data[index]:
            masked_indexes.append(index)

    mi_i = 0
    for i, item in enumerate(data['text']):
        if data['text'][i] != '' and data['text'][i] != ' ' and data['text'][i] != '  ':
            if mi_i in masked_indexes:
                sorted_indexes.append(i)

            mi_i += 1

    for i in range(len(data['level'])):
        if i in sorted_indexes:
            height, width, _ = img.shape
            text = [int(data['left'][i]), int(data['top'][i]), int(data['width'][i]), int(data['height'][i])]
            cv2.rectangle(img, (text[0], text[1]), (text[0] + text[2], text[1] + text[3]), (0, 0, 0), -1)

    cv2.imwrite(output_path, img)
