from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder


from src.scripts import auth
from src.scripts.book import Book
from src.scripts.book_messages import BookMessages
from src.scripts.book_stats import BookStats
from src.scripts.exceptions import BadCredentials, ObjectNotFound, UsernameNotUnique
from src.constants import TOP_LIST

from src.scripts.book_list import BookList
from src.scripts.message import Message
from src.scripts.search import Search
from src.scripts.user_list import UserList
from src.scripts.user_stats import UserStats
from src.scripts.status import Status
from src.scripts.score import Score



# from fastapi.templates import Jinja2Templates
router = APIRouter()


templates = Jinja2Templates(directory="src/frontend/html")


@router.get(
    "/",
    response_class=HTMLResponse,
    summary="Main page",
    description="Displays the application's home page with basic information",
    response_description="HTML page with main content"
)
async def root(request: Request):
    user_data = auth.get_user_data(request)
    return templates.TemplateResponse(
        "index.html", {"request": request, "user_data": user_data}
    )


@router.get(
    "/catalog",
    response_class=HTMLResponse,
    summary="Book catalog",
    description="Displays a paginated list of books with filtering options (Top, Recommendations, etc.)",
    response_description="HTML page with book catalog"
    )
async def catalog(request: Request, filter: str = "Top", page: int = 0):
    user_data = auth.get_user_data(request)
    try:
        if filter == "Recommendations":
            book_lists = BookList(filter, user_data["preferred_username"])
            book_list, pages = book_lists.get_recommendation_book_list(page)
        else:
            book_lists = BookList(filter)
            book_list, pages = book_lists.get_book_list(page)
    except:
        book_list, pages = [], 0

    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user_data": user_data,
            "books": book_list,
            "current_filter": filter,
            "current_page": page,
            "pages": pages,
            "filter": filter,
        },
    )


@router.get(
    "/personal",
    response_class=HTMLResponse,
    summary="Personal account",
    description="Displays a user's personal account with information about their reading lists and statistics",
    response_description="HTML page with personal account"

)
async def personal(request: Request):
    user_data = auth.get_user_data(request)
    user_lists = UserList(user_data["preferred_username"])
    completed = user_lists.get_completed_list()
    reading = user_lists.get_reading_list()
    planned = user_lists.get_planned_list()
    user_stats = UserStats(user_data["preferred_username"])
    return templates.TemplateResponse(
        "personal_account.html",
        {
            "request": request,
            "user_data": user_data,
            "user_lists": [completed, reading, planned],
            "user_stats": user_stats,
        },
    )


@router.post(
    "/book",
    summary="Post a book comment",
    description="Allows authenticated users to post comments about a specific book",
    response_description="Redirects to book page with new comment"
)
async def book_post(request: Request, id: int, page: int = 0, comment: str = Form(...)):
    user_data = auth.get_user_data(request)

    try:
        Book(id)
    except ObjectNotFound:
        return templates.TemplateResponse(
            "not_found.html", {"request": request, "user_data": user_data}
        )

    if user_data != {}:
        username = user_data["preferred_username"]
        Message(username, id, comment).set_message()

    return await book(request, id, page)


@router.get(
    "/book",
    response_class=HTMLResponse,
    summary="Book details",
    description="Displays detailed information about a specific book including comments, ratings, and user status",
    response_description="HTML page with book details"
)
async def book(request: Request, id: int, page: int = 0):
    user_data = auth.get_user_data(request)

    try:
        book = Book(id)
    except ObjectNotFound:
        return templates.TemplateResponse(
            "not_found.html", {"request": request, "user_data": user_data}
        )

    book_stats = BookStats(id)
    comments = BookMessages(id)
    comments = (comments.get_book_comments(page), comments.get_pages_count())

    if not user_data:
        return templates.TemplateResponse(
            "book_info.html",
            {
                "request": request,
                "user_data": {},
                "book": book,
                "status": None,
                "score": None,
                "comments_allowed": False,
                "book_stats": book_stats,
                "comments": comments,
                "current_page": page,
            },
        )

    status = Status(user_data["preferred_username"], id).status
    if not status:
        status = "untracked"

    score = Score(user_data["preferred_username"], id).score
    if not score:
        score = 0

    return templates.TemplateResponse(
        "book_info.html",
        {
            "request": request,
            "user_data": user_data,
            "book": book,
            "status": status,
            "score": score,
            "comments_allowed": True,
            "book_stats": book_stats,
            "comments": comments,
            "current_page": page,
        },
    )


@router.post(
    "/search",
    response_class=HTMLResponse,
     summary="Search books",
    description="Performs a search across books using Levenshtein distance algorithm",
    response_description="HTML page with search results"
)
async def search(request: Request, search_string: str = Form(...)):

    import subprocess

    result = subprocess.Popen(
        ["./levenshtein_length"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd="src/scripts/searching_mechanism",
    )
    # from src.microservices.recommendation_system_project import search_engine
    # cleaned_lines = search_engine.search(search_string)
    output_data, stderr_data = result.communicate(input=search_string + "\n")
    output_lines = output_data.splitlines()

    cleaned_lines = [
        line.strip()
        for line in output_lines
        if line.strip() and not line.startswith("----")
    ]

    search_instance = Search(cleaned_lines)
    books = search_instance.get_search_result()

    user_data = auth.get_user_data(request)

    return templates.TemplateResponse(
        "search_results.html",
        {
            "request": request,
            "user_data": user_data,
            "books": books,
            "query": search_string,
        },
    )


@router.get(
    "/signin",
    response_class=HTMLResponse,
    summary="Sign in page",
    description="Displays the login form for existing users",
    response_description="HTML page with login form"
)
async def signin_get(request: Request):
    return templates.TemplateResponse(
        "signin.html", {"request": request, "saved_values": {}}
    )


@router.get(
    "/lost",
    response_class=HTMLResponse,
    summary="Not found page",
    description="Displays a 404 error page when content is not found",
    response_description="HTML page with 404 error"
)
async def lost(request: Request):
    user_data = auth.get_user_data(request)
    return templates.TemplateResponse(
        "not_found.html", {"request": request, "user_data": user_data}
    )


@router.get(
    "/registration",
    summary="Registration page",
    description="Displays the registration form for new users",
    response_description="HTML page with registration form",
    response_class=HTMLResponse,
)
async def register(request: Request):
    return templates.TemplateResponse(
        "registration.html", {"request": request, "saved_values": {}}
    )


@router.post(
    "/registration",
    response_class=HTMLResponse,
    summary="Register new user",
    description="Processes user registration with username, email, and password",
    response_description="Redirects to sign in page"
)
async def register_post(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
):
    username = username.lower()
    if password != password_confirm:
        return templates.TemplateResponse(
            "registration.html",
            {
                "request": request,
                "error": "passwords don't match",
                "saved_values": {
                    "username": username,
                    "email": email,
                    "password": password,
                },
            },
        )

    try:
        auth.create_user(username, password, email)
    except UsernameNotUnique:
        return templates.TemplateResponse(
            "registration.html",
            {
                "request": request,
                "error": "This username is already taken, try another one",
                "saved_values": {
                    "username": username,
                    "email": email,
                    "password": password,
                },
            },
        )

    return await signin_post(request, username, password)


@router.post("/signin",
    summary="Authenticate user",
    description="Processes user authentication with username and password",
    response_description="Sets auth cookies and redirects to personal account"
)
async def signin_post(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    username = username.lower()
    try:
        access, refresh = auth.authenticate(username=username, password=password)
    except BadCredentials:
        return templates.TemplateResponse(
            "signin.html",
            {
                "request": request,
                "error": "wrong password or username",
                "saved_values": {"username": username, "password": password},
            },
        )

    response = RedirectResponse("/personal", status_code=303)
    response.set_cookie("access", access)
    response.set_cookie("refresh", refresh)
    return response


@router.post(
        "/search",
        response_class=HTMLResponse,
        
)
async def search_post(request: Request, search_string: str = Form(...)):
    search_results = [
        {"title": "Found book", "author": "Author", "cover": "/img/book_cover.jpg"}
    ]

    return templates.TemplateResponse(
        "search_results.html",
        {"request": request, "results": search_results, "query": search_string},
    )


@router.get("/logout",
    summary="Log out user",
    description="Clears authentication cookies and logs out the user",
    response_description="Redirects to home page"
)
async def logout(request: Request):
    response = RedirectResponse("/", status_code=303)
    refresh = request.cookies.get("refresh")
    if not refresh:
        return response
    auth.logout(refresh)
    response.delete_cookie("refresh")
    response.delete_cookie("access")
    return response
